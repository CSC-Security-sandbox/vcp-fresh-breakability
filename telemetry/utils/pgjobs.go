package utils

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"log"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
)

const (
	JOB_STATUS_SCHEDULED = "new"
	JOB_STATUS_FINISHED  = "finished"
	JOB_STATUS_FAILED    = "failed"

	MAX_RETRY = 3
)

var PollInterval = 1 * time.Second
var JobsTableName = "jobs"

type Job interface {
	Perform(processor interface{}, attempt int32) error
	Load(data string) (Job, error)
}

type JobQueue struct {
	db        *sql.DB
	processor interface{}

	mutex        sync.Mutex
	typeRegistry map[string]reflect.Type
}

func NewQueue(db *sql.DB, p interface{}) *JobQueue {
	return &JobQueue{
		db:           db,
		processor:    p,
		typeRegistry: make(map[string]reflect.Type),
	}
}

func (j *JobQueue) EnqueueAt(ctx context.Context, job Job, queue string, at time.Time) error {
	typeName := j.typeName(job)

	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("queue: failed marshaling: %v", err)
	}

	if _, err = j.db.ExecContext(
		ctx,
		`INSERT INTO `+JobsTableName+` (type_name, status, queue, data, scheduled_at) VALUES ($1, $2, $3, $4, $5)`,
		typeName,
		JOB_STATUS_SCHEDULED,
		queue,
		data,
		at,
	); err != nil {
		return fmt.Errorf("queue: failed inserting job: %w", err)
	}

	return nil
}

func (j *JobQueue) Enqueue(ctx context.Context, job Job, queue string) error {
	logger := util.GetLogger(ctx)
	logger.Debugf("queue: enqueing queue=%v job=%+v", queue, job)

	typeName := j.typeName(job)

	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("queue: failed marshaling: %v", err)
	}

	if _, err = j.db.ExecContext(
		ctx,
		`INSERT INTO `+JobsTableName+` (type_name, status, queue, data) VALUES ($1, $2, $3, $4)`,
		typeName,
		JOB_STATUS_SCHEDULED,
		queue,
		data,
	); err != nil {
		return fmt.Errorf("queue: failed inserting job: %w", err)
	}

	return nil
}

func (j *JobQueue) Dequeue(ctx context.Context, queues []string) error {
	var job datamodel.Job
	logger := util.GetLogger(ctx)

	sqlStmt := ` 
		UPDATE
		  ` + JobsTableName + `
		SET
		  status = $1,
		  started_at = clock_timestamp(),
		  attempt = attempt + 1
		WHERE
		  id IN (
			SELECT
			  id FROM ` + JobsTableName + ` j
			WHERE
			  (j.status = $2 or (j.status = $3 and j.attempt < $4))
			  AND j.queue = any($5)
			  AND j.type_name = any($6) 
			  AND (j.scheduled_at is null or (j.scheduled_at <= now()))
			ORDER BY
			  j.scheduled_at, j.created_at
			FOR UPDATE SKIP LOCKED
		  LIMIT 1)
		RETURNING id, type_name, data, attempt
	`

	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	queueArray, err := pqArray(queues)
	if err != nil {
		return err
	}

	typesArray, err := pqArray(mapKeys(j.typeRegistry))
	if err != nil {
		return err
	}

	row := tx.QueryRowContext(
		ctx,
		sqlStmt,
		JOB_STATUS_FINISHED,
		JOB_STATUS_SCHEDULED,
		JOB_STATUS_FAILED,
		MAX_RETRY,
		queueArray,
		typesArray,
	)
	err = row.Scan(&job.ID, &job.TypeName, &job.Data, &job.Attempt)
	if err == sql.ErrNoRows {
		return nil
	} else if err != nil {
		return err
	}

	logger.Debugf("Dequeued job: id=%v type=%v attempt=%v queue=%v", job.ID, job.TypeName, job.Attempt, queues)

	// get original go type based on type name
	jobType, err := j.getType(job.TypeName)
	if err != nil {
		_, err = tx.ExecContext(ctx, `UPDATE `+JobsTableName+` SET status = $1, finished_at = clock_timestamp(), error = $3 WHERE id = $2`, JOB_STATUS_FAILED, job.ID, err.Error())
		if err != nil {
			return fmt.Errorf("unable to exec error for failed job %v", err)
		}

		if err = tx.Commit(); err != nil {
			return fmt.Errorf("unable to commit error for failed job %v", err)
		}

		return fmt.Errorf("unable to find related job '%v': %v", job.TypeName, err)
	}

	// create a new object by unmarshaling the job data
	loadedJob, err := jobType.Load(job.Data)
	if err != nil {
		return err
	}
	// execute job
	err = loadedJob.Perform(j.processor, int32(job.Attempt))
	if err != nil {
		// TODO: add retry handling and save error to job row
		_, err = tx.ExecContext(ctx, `UPDATE `+JobsTableName+` SET status = $1, finished_at = clock_timestamp(), error = $3 WHERE id = $2`, JOB_STATUS_FAILED, job.ID, err.Error())
		if err != nil {
			return err
		}
		return tx.Commit()
	}

	_, err = tx.ExecContext(ctx, `UPDATE `+JobsTableName+` SET status = $1, finished_at = clock_timestamp() WHERE id = $2`, JOB_STATUS_FINISHED, job.ID)
	if err != nil {
		return fmt.Errorf("failed updating job status: %w", err)
	}

	return tx.Commit()
}

func (j *JobQueue) Worker(ctx context.Context, queues []string, types ...interface{}) error {
	// register all passed types in a type type registry.
	// this allows to map job types back to their corresponding go type
	// to execute the Perform() action.
	for _, t := range types {
		j.registerType(t)
	}

	tm := time.NewTicker(PollInterval)
	defer tm.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Println("job queue worker stopped")
			return ctx.Err()
		case <-tm.C:
			if err := j.Dequeue(ctx, queues); err != nil {
				log.Println("queue: dequeue failed", err)
			}
		}
	}
}

func (j *JobQueue) typeName(typedNil interface{}) string {
	name := reflect.TypeOf(typedNil).String()
	name = strings.TrimPrefix(name, "*")

	return name
}

func (j *JobQueue) registerType(typedNil interface{}) {
	t := reflect.TypeOf(typedNil).Elem()
	name := j.typeName(typedNil)

	j.mutex.Lock()
	defer j.mutex.Unlock()
	j.typeRegistry[name] = t
}

func (j *JobQueue) getType(name string) (Job, error) {
	item, ok := j.typeRegistry[name]

	if !ok {
		return nil, fmt.Errorf("type not found in type registry. did you register the job?")
	}

	t := reflect.New(item).Elem().Interface().(Job)

	return t, nil
}

// pqArray and appendArrayQuotedBytes func extracted from https://github.com/lib/pq
// to remove dependency on lib/pq
func pqArray(a []string) (string, error) {
	if n := len(a); n > 0 {
		// There will be at least two curly brackets, 2*N bytes of quotes,
		// and N-1 bytes of delimiters.
		b := make([]byte, 1, 1+3*n)
		b[0] = '{'

		b = appendArrayQuotedBytes(b, []byte(a[0]))
		for i := 1; i < n; i++ {
			b = append(b, ',')
			b = appendArrayQuotedBytes(b, []byte(a[i]))
		}

		return string(append(b, '}')), nil
	}

	return "{}", nil
}

func appendArrayQuotedBytes(b, v []byte) []byte {
	b = append(b, '"')
	for {
		i := bytes.IndexAny(v, `"\`)
		if i < 0 {
			b = append(b, v...)
			break
		}
		if i > 0 {
			b = append(b, v[:i]...)
		}
		b = append(b, '\\', v[i])
		v = v[i+1:]
	}
	return append(b, '"')
}

func mapKeys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
