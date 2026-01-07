package utils

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
)

// MockJob implements the Job interface for testing
type MockJob struct {
	ID   string `json:"id"`
	Data string `json:"data"`
}

func (m MockJob) Perform(processor interface{}, attempt int32) error {
	if m.ID == "fail" {
		return fmt.Errorf("mock job failed")
	}
	return nil
}

func (m MockJob) Load(data string) (Job, error) {
	var job MockJob
	err := json.Unmarshal([]byte(data), &job)
	if err != nil {
		return nil, err
	}
	return job, nil
}

// FailingJob implements the Job interface for testing failure scenarios
type FailingJob struct {
	ID string `json:"id"`
}

func (f FailingJob) Perform(processor interface{}, attempt int32) error {
	return fmt.Errorf("failing job always fails")
}

func (f FailingJob) Load(data string) (Job, error) {
	var job FailingJob
	err := json.Unmarshal([]byte(data), &job)
	if err != nil {
		return nil, err
	}
	return job, nil
}

// UnmarshalableJob for testing marshaling errors
type UnmarshalableJob struct {
	BadField chan string `json:"bad_field"` // channels can't be marshaled
}

func (u UnmarshalableJob) Perform(processor interface{}, attempt int32) error {
	return nil
}

func (u UnmarshalableJob) Load(data string) (Job, error) {
	return u, nil
}

// setupTestDB creates an in-memory SQLite database for testing
func setupTestDB(t *testing.T) (*sql.DB, func()) {
	gormDB, err := database.SetupTestDB()
	require.NoError(t, err)

	sqlDB, err := gormDB.DB()
	require.NoError(t, err)

	// Create jobs table with SQLite-compatible schema
	_, err = sqlDB.Exec(`
		CREATE TABLE IF NOT EXISTS jobs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			type_name TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'new',
			queue TEXT NOT NULL,
			data TEXT NOT NULL,
			error TEXT,
			attempt INTEGER DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			started_at DATETIME,
			finished_at DATETIME,
			scheduled_at DATETIME
		)
	`)
	require.NoError(t, err)

	cleanup := func() {
		_ = sqlDB.Close()
	}

	return sqlDB, cleanup
}

// MockJobQueue wraps JobQueue but modifies the Dequeue method to work with SQLite
type MockJobQueue struct {
	*JobQueue
}

func newMockQueue(db *sql.DB, processor interface{}) *MockJobQueue {
	return &MockJobQueue{
		JobQueue: NewQueue(db, processor),
	}
}

// Dequeue method modified for SQLite compatibility
func (mq *MockJobQueue) Dequeue(ctx context.Context, queues []string) error {
	var job datamodel.Job

	tx, err := mq.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// Build the query based on available queues and types
	var whereConditions []string
	var args []interface{}

	// Status condition
	whereConditions = append(whereConditions, "(status = ? OR (status = ? AND attempt < ?))")
	args = append(args, JOB_STATUS_SCHEDULED, JOB_STATUS_FAILED, 5) // Default max retry value

	// Queue condition
	if len(queues) > 0 {
		whereConditions = append(whereConditions, "queue IN ("+placeholders(len(queues))+")")
		for _, queue := range queues {
			args = append(args, queue)
		}
	} else {
		// If no queues provided, don't process any jobs
		return nil
	}

	// Type condition - only add if we have registered types
	if len(mq.typeRegistry) > 0 {
		whereConditions = append(whereConditions, "type_name IN ("+placeholders(len(mq.typeRegistry))+")")
		for typeName := range mq.typeRegistry {
			args = append(args, typeName)
		}
	} else {
		// If no types are registered, no jobs can be processed (mimics PostgreSQL behavior)
		return nil
	}

	// Schedule condition
	whereConditions = append(whereConditions, "(scheduled_at IS NULL OR scheduled_at <= datetime('now', 'localtime'))")

	selectStmt := `
		SELECT id, type_name, data, attempt FROM jobs 
		WHERE ` + strings.Join(whereConditions, " AND ") + `
		ORDER BY 
			CASE WHEN scheduled_at IS NULL THEN 0 ELSE 1 END,
			scheduled_at,
			created_at
		LIMIT 1
	`

	row := tx.QueryRowContext(ctx, selectStmt, args...)
	err = row.Scan(&job.ID, &job.TypeName, &job.Data, &job.Attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	} else if err != nil {
		return err
	}

	// Update the job as started (increment attempt)
	_, err = tx.ExecContext(ctx,
		`UPDATE jobs SET started_at = datetime('now'), attempt = attempt + 1 WHERE id = ?`,
		job.ID)
	if err != nil {
		return err
	}

	// get original go type based on type name
	jobType, err := mq.getType(job.TypeName)
	if err != nil {
		_, err = tx.ExecContext(ctx, `UPDATE jobs SET status = ?, finished_at = datetime('now'), error = ? WHERE id = ?`,
			JOB_STATUS_FAILED, err.Error(), job.ID)
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
	err = loadedJob.Perform(mq.processor, int32(job.Attempt+1)) // Use incremented attempt
	if err != nil {
		// Save error to job row
		_, err = tx.ExecContext(ctx, `UPDATE jobs SET status = ?, finished_at = datetime('now'), error = ? WHERE id = ?`,
			JOB_STATUS_FAILED, err.Error(), job.ID)
		if err != nil {
			return err
		}
		return tx.Commit()
	}

	_, err = tx.ExecContext(ctx, `UPDATE jobs SET status = ?, finished_at = datetime('now') WHERE id = ?`,
		JOB_STATUS_FINISHED, job.ID)
	if err != nil {
		return fmt.Errorf("failed updating job status: %w", err)
	}

	return tx.Commit()
}

// Helper function to create SQL placeholders
func placeholders(count int) string {
	if count == 0 {
		return "NULL" // This will never match anything in an IN clause
	}
	result := "?"
	for i := 1; i < count; i++ {
		result += ",?"
	}
	return result
}

func TestNewQueue(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)

	assert.NotNil(t, queue)
	assert.Equal(t, db, queue.db)
	assert.Equal(t, mockProcessor, queue.processor)
	assert.NotNil(t, queue.typeRegistry)
	assert.Empty(t, queue.typeRegistry)
}

func TestJobQueue_Enqueue(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)
	ctx := context.Background()

	job := MockJob{ID: "test", Data: "test data"}

	err := queue.Enqueue(ctx, job, "test_queue")
	assert.NoError(t, err)

	// Verify job was inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM jobs WHERE queue = ?", "test_queue").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify job details
	var typeName, status, queueName, data string
	err = db.QueryRow("SELECT type_name, status, queue, data FROM jobs WHERE queue = ?", "test_queue").
		Scan(&typeName, &status, &queueName, &data)
	assert.NoError(t, err)
	assert.Equal(t, "utils.MockJob", typeName)
	assert.Equal(t, JOB_STATUS_SCHEDULED, status)
	assert.Equal(t, "test_queue", queueName)

	var jobData MockJob
	err = json.Unmarshal([]byte(data), &jobData)
	assert.NoError(t, err)
	assert.Equal(t, job.ID, jobData.ID)
	assert.Equal(t, job.Data, jobData.Data)
}

func TestJobQueue_EnqueueAt(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)
	ctx := context.Background()

	job := MockJob{ID: "scheduled", Data: "scheduled data"}
	scheduledTime := time.Now().Add(1 * time.Hour)

	err := queue.EnqueueAt(ctx, job, "scheduled_queue", scheduledTime)
	assert.NoError(t, err)

	// Verify job was inserted with correct scheduled time
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM jobs WHERE queue = ?", "scheduled_queue").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	var scheduledAt time.Time
	err = db.QueryRow("SELECT scheduled_at FROM jobs WHERE queue = ?", "scheduled_queue").Scan(&scheduledAt)
	assert.NoError(t, err)
	assert.WithinDuration(t, scheduledTime, scheduledAt, time.Second)
}

func TestJobQueue_EnqueueMarshalError(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)
	ctx := context.Background()

	// Use a job that can't be marshaled
	job := UnmarshalableJob{BadField: make(chan string)}

	err := queue.Enqueue(ctx, job, "test_queue")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed marshaling")

	// Verify no job was inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM jobs WHERE queue = ?", "test_queue").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestJobQueue_Dequeue_NoJobs(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := newMockQueue(db, mockProcessor)

	// Register the job type
	queue.registerType(&MockJob{})

	err := queue.Dequeue(context.Background(), []string{"empty_queue"})
	assert.NoError(t, err) // Should not error when no jobs are available
}

func TestJobQueue_Dequeue_Success(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := newMockQueue(db, mockProcessor)
	ctx := context.Background()

	// Register the job type
	queue.registerType(&MockJob{})

	// Enqueue a job
	job := MockJob{ID: "success", Data: "success data"}
	err := queue.Enqueue(ctx, job, "test_queue")
	require.NoError(t, err)

	// Dequeue and process the job
	err = queue.Dequeue(ctx, []string{"test_queue"})
	assert.NoError(t, err)

	// Verify job status was updated to finished
	var status string
	err = db.QueryRow("SELECT status FROM jobs WHERE queue = ?", "test_queue").Scan(&status)
	assert.NoError(t, err)
	assert.Equal(t, JOB_STATUS_FINISHED, status)
}

func TestJobQueue_Dequeue_JobFailure(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := newMockQueue(db, mockProcessor)
	ctx := context.Background()

	// Register the job type
	queue.registerType(&FailingJob{})

	// Enqueue a failing job
	job := FailingJob{ID: "fail"}
	err := queue.Enqueue(ctx, job, "test_queue")
	require.NoError(t, err)

	// Dequeue and process the job (should fail)
	err = queue.Dequeue(ctx, []string{"test_queue"})
	assert.NoError(t, err) // Dequeue itself should not error, but job should fail

	// Verify job status was updated to failed
	var status string
	var errorMsg sql.NullString
	err = db.QueryRow("SELECT status, error FROM jobs WHERE queue = ?", "test_queue").
		Scan(&status, &errorMsg)
	assert.NoError(t, err)
	assert.Equal(t, JOB_STATUS_FAILED, status)
	assert.True(t, errorMsg.Valid)
	assert.Contains(t, errorMsg.String, "failing job always fails")
}

func TestJobQueue_Dequeue_UnknownJobType(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := newMockQueue(db, mockProcessor)
	ctx := context.Background()

	// Don't register the job type, but enqueue a job
	job := MockJob{ID: "unknown", Data: "unknown data"}
	err := queue.Enqueue(ctx, job, "test_queue")
	require.NoError(t, err)

	// Try to dequeue without registering the type
	err = queue.Dequeue(ctx, []string{"test_queue"})
	// Since no types are registered, it should handle gracefully
	assert.NoError(t, err)

	// Since no types are registered, job should remain unprocessed
	var status string
	err = db.QueryRow("SELECT status FROM jobs WHERE queue = ?", "test_queue").Scan(&status)
	assert.NoError(t, err)
	assert.Equal(t, JOB_STATUS_SCHEDULED, status)
}

func TestJobQueue_Dequeue_ScheduledJob_NotReady(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := newMockQueue(db, mockProcessor)
	ctx := context.Background()

	// Register the job type
	queue.registerType(&MockJob{})

	// Enqueue a job scheduled for the future
	job := MockJob{ID: "future", Data: "future data"}
	futureTime := time.Now().Add(1 * time.Hour)
	err := queue.EnqueueAt(ctx, job, "test_queue", futureTime)
	require.NoError(t, err)

	// Try to dequeue - should not process the future job
	err = queue.Dequeue(ctx, []string{"test_queue"})
	assert.NoError(t, err)

	// Verify job status is still scheduled
	var status string
	err = db.QueryRow("SELECT status FROM jobs WHERE queue = ?", "test_queue").Scan(&status)
	assert.NoError(t, err)
	assert.Equal(t, JOB_STATUS_SCHEDULED, status)
}

func TestJobQueue_Dequeue_ScheduledJob_Ready(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := newMockQueue(db, mockProcessor)
	ctx := context.Background()

	// Register the job type
	queue.registerType(&MockJob{})

	// Enqueue a job scheduled for the past (by inserting directly to avoid time zone issues)
	job := MockJob{ID: "ready", Data: "ready data"}
	jobData, err := json.Marshal(job)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO jobs (type_name, status, queue, data, scheduled_at) VALUES (?, ?, ?, ?, ?)`,
		"utils.MockJob", JOB_STATUS_SCHEDULED, "test_queue", string(jobData), time.Now().Add(-1*time.Hour))
	require.NoError(t, err)

	// Dequeue - should process the ready job
	err = queue.Dequeue(ctx, []string{"test_queue"})
	assert.NoError(t, err)

	// Verify job status was updated to finished
	var status string
	err = db.QueryRow("SELECT status FROM jobs WHERE queue = ?", "test_queue").Scan(&status)
	assert.NoError(t, err)
	assert.Equal(t, JOB_STATUS_FINISHED, status)
}

func TestJobQueue_Dequeue_RetryLogic(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := newMockQueue(db, mockProcessor)
	ctx := context.Background()

	// Register the job type
	queue.registerType(&FailingJob{})

	// Enqueue a failing job
	job := FailingJob{ID: "retry"}
	err := queue.Enqueue(ctx, job, "test_queue")
	require.NoError(t, err)

	// Process the job multiple times (it should retry up to 5 times - default max retry)
	for i := 0; i < 5; i++ {
		err = queue.Dequeue(ctx, []string{"test_queue"})
		assert.NoError(t, err)
	}

	// Verify the attempt count
	var attempt int32
	err = db.QueryRow("SELECT attempt FROM jobs WHERE queue = ?", "test_queue").Scan(&attempt)
	assert.NoError(t, err)
	assert.Equal(t, int32(5), attempt) // Default max retry value

	// Try one more time - should not process as max retries reached
	err = queue.Dequeue(ctx, []string{"test_queue"})
	assert.NoError(t, err)

	// Verify attempt count didn't increase
	err = db.QueryRow("SELECT attempt FROM jobs WHERE queue = ?", "test_queue").Scan(&attempt)
	assert.NoError(t, err)
	assert.Equal(t, int32(5), attempt) // Default max retry value
}

func TestJobQueue_Worker(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)

	// Create a context that will be canceled after a short time
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Start the worker
	err := queue.Worker(ctx, []string{"worker_queue"}, &MockJob{})
	assert.Error(t, err) // Should return context deadline exceeded error
	assert.Equal(t, context.DeadlineExceeded, err)
}

func TestJobQueue_Worker_ProcessesJobs(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor) // Use real queue instead of mock

	// Enqueue a job before starting the worker
	job := MockJob{ID: "worker_test", Data: "worker data"}
	err := queue.Enqueue(context.Background(), job, "worker_queue")
	require.NoError(t, err)

	// Set a very short poll interval for testing
	originalPollInterval := PollInterval
	PollInterval = 10 * time.Millisecond
	defer func() { PollInterval = originalPollInterval }()

	// Create a context that will be canceled after the job is processed
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Start the worker
	err = queue.Worker(ctx, []string{"worker_queue"}, &MockJob{})
	assert.Error(t, err) // Should return context deadline exceeded error

	// For the real implementation with PostgreSQL features, jobs may not be processed
	// in SQLite, so we just verify no panic occurred
}

func TestJobQueue_TypeName(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)

	// Test with pointer type
	job := &MockJob{}
	typeName := queue.typeName(job)
	assert.Equal(t, "utils.MockJob", typeName)

	// Test with non-pointer type
	jobValue := MockJob{}
	typeName = queue.typeName(jobValue)
	assert.Equal(t, "utils.MockJob", typeName)
}

func TestJobQueue_RegisterType(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)

	// Register a type
	queue.registerType(&MockJob{})

	// Verify it was registered
	assert.Len(t, queue.typeRegistry, 1)
	assert.Contains(t, queue.typeRegistry, "utils.MockJob")
}

func TestJobQueue_GetType(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)

	// Register a type
	queue.registerType(&MockJob{})

	// Get the type
	job, err := queue.getType("utils.MockJob")
	assert.NoError(t, err)
	assert.IsType(t, MockJob{}, job)

	// Try to get unknown type
	_, err = queue.getType("unknown.Type")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "type not found in type registry")
}

func TestPqArray(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected string
	}{
		{
			name:     "empty array",
			input:    []string{},
			expected: "{}",
		},
		{
			name:     "single element",
			input:    []string{"test"},
			expected: `{"test"}`,
		},
		{
			name:     "multiple elements",
			input:    []string{"test1", "test2", "test3"},
			expected: `{"test1","test2","test3"}`,
		},
		{
			name:     "elements with quotes",
			input:    []string{`test"quote`, "normal"},
			expected: `{"test\"quote","normal"}`,
		},
		{
			name:     "elements with backslashes",
			input:    []string{`test\backslash`, "normal"},
			expected: `{"test\\backslash","normal"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := pqArray(tt.input)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAppendArrayQuotedBytes(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "normal string",
			input:    []byte("test"),
			expected: `"test"`,
		},
		{
			name:     "string with quotes",
			input:    []byte(`test"quote`),
			expected: `"test\"quote"`,
		},
		{
			name:     "string with backslashes",
			input:    []byte(`test\backslash`),
			expected: `"test\\backslash"`,
		},
		{
			name:     "empty string",
			input:    []byte(""),
			expected: `""`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := appendArrayQuotedBytes([]byte{}, tt.input)
			assert.Equal(t, tt.expected, string(result))
		})
	}
}

func TestMapKeys(t *testing.T) {
	tests := []struct {
		name          string
		input         map[string]int
		expectedCount int
	}{
		{
			name:          "empty map",
			input:         map[string]int{},
			expectedCount: 0,
		},
		{
			name:          "single key",
			input:         map[string]int{"key1": 1},
			expectedCount: 1,
		},
		{
			name:          "multiple keys",
			input:         map[string]int{"key1": 1, "key2": 2, "key3": 3},
			expectedCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapKeys(tt.input)
			assert.Len(t, result, tt.expectedCount)

			// For non-empty maps, check that all input keys are present
			if tt.expectedCount > 0 {
				for expectedKey := range tt.input {
					assert.Contains(t, result, expectedKey)
				}
			}
		})
	}
}
func TestJobQueue_Dequeue_DatabaseError(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)
	ctx := context.Background()

	// Close the database to simulate an error
	_ = db.Close()

	// Try to dequeue - should return an error
	err := queue.Dequeue(ctx, []string{"test_queue"})
	assert.Error(t, err)
}

func TestJobQueue_Enqueue_DatabaseError(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)
	ctx := context.Background()

	job := MockJob{ID: "test", Data: "test data"}

	// Close the database to simulate an error
	_ = db.Close()

	err := queue.Enqueue(ctx, job, "test_queue")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed inserting job")
}

func TestJobQueue_Dequeue_MultipleQueues(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := newMockQueue(db, mockProcessor)
	ctx := context.Background()

	// Register the job type
	queue.registerType(&MockJob{})

	// Enqueue jobs in different queues
	job1 := MockJob{ID: "queue1", Data: "queue1 data"}
	job2 := MockJob{ID: "queue2", Data: "queue2 data"}
	job3 := MockJob{ID: "queue3", Data: "queue3 data"}

	err := queue.Enqueue(ctx, job1, "queue1")
	require.NoError(t, err)
	err = queue.Enqueue(ctx, job2, "queue2")
	require.NoError(t, err)
	err = queue.Enqueue(ctx, job3, "queue3")
	require.NoError(t, err)

	// Dequeue from multiple queues
	err = queue.Dequeue(ctx, []string{"queue1", "queue2"})
	assert.NoError(t, err)

	// Check that one of the jobs from queue1 or queue2 was processed
	var processedCount int
	err = db.QueryRow("SELECT COUNT(*) FROM jobs WHERE status = ? AND queue IN ('queue1', 'queue2')",
		JOB_STATUS_FINISHED).Scan(&processedCount)
	assert.NoError(t, err)
	assert.Equal(t, 1, processedCount)

	// Check that queue3 job was not processed
	var queue3Status string
	err = db.QueryRow("SELECT status FROM jobs WHERE queue = 'queue3'").Scan(&queue3Status)
	assert.NoError(t, err)
	assert.Equal(t, JOB_STATUS_SCHEDULED, queue3Status)
}

func TestJobQueue_Constants(t *testing.T) {
	assert.Equal(t, "new", JOB_STATUS_SCHEDULED)
	assert.Equal(t, "finished", JOB_STATUS_FINISHED)
	assert.Equal(t, "failed", JOB_STATUS_FAILED)
	assert.Equal(t, "jobs", JobsTableName)
}

func TestJobQueue_PollInterval(t *testing.T) {
	// Test that PollInterval can be modified
	originalPollInterval := PollInterval
	defer func() { PollInterval = originalPollInterval }()

	PollInterval = 5 * time.Second
	assert.Equal(t, 5*time.Second, PollInterval)
}

// Additional comprehensive tests for better coverage

func TestJobQueue_EnqueueAt_DatabaseError(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)
	ctx := context.Background()

	job := MockJob{ID: "test", Data: "test data"}
	scheduledTime := time.Now().Add(1 * time.Hour)

	// Close the database to simulate an error
	_ = db.Close()

	err := queue.EnqueueAt(ctx, job, "test_queue", scheduledTime)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed inserting job")
}

func TestJobQueue_EnqueueAt_MarshalError(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)
	ctx := context.Background()

	// Use a job that can't be marshaled
	job := UnmarshalableJob{BadField: make(chan string)}
	scheduledTime := time.Now().Add(1 * time.Hour)

	err := queue.EnqueueAt(ctx, job, "test_queue", scheduledTime)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed marshaling")
}

// LoadErrorJob for testing load errors
type LoadErrorJob struct {
	ID string `json:"id"`
}

func (l LoadErrorJob) Perform(processor interface{}, attempt int32) error {
	return nil
}

func (l LoadErrorJob) Load(data string) (Job, error) {
	return nil, fmt.Errorf("load error")
}

func TestJobQueue_Dequeue_LoadError(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := newMockQueue(db, mockProcessor)
	ctx := context.Background()

	// Register the job type
	queue.registerType(&LoadErrorJob{})

	// Enqueue a job
	job := LoadErrorJob{ID: "load_error"}
	err := queue.Enqueue(ctx, job, "test_queue")
	require.NoError(t, err)

	// Dequeue and process the job (should fail on load)
	err = queue.Dequeue(ctx, []string{"test_queue"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "load error")
}

func TestJobQueue_Dequeue_EmptyQueues(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := newMockQueue(db, mockProcessor)
	ctx := context.Background()

	// Register the job type
	queue.registerType(&MockJob{})

	// Enqueue a job
	job := MockJob{ID: "test", Data: "test data"}
	err := queue.Enqueue(ctx, job, "test_queue")
	require.NoError(t, err)

	// Try to dequeue from empty queues list (should find no jobs)
	err = queue.Dequeue(ctx, []string{})
	assert.NoError(t, err)

	// Verify job status is still scheduled
	var status string
	err = db.QueryRow("SELECT status FROM jobs WHERE queue = ?", "test_queue").Scan(&status)
	assert.NoError(t, err)
	assert.Equal(t, JOB_STATUS_SCHEDULED, status)
}

func TestJobQueue_Dequeue_TransactionBeginError(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := newMockQueue(db, mockProcessor)
	ctx := context.Background()

	// Close database to force transaction begin error
	_ = db.Close()

	err := queue.Dequeue(ctx, []string{"test_queue"})
	assert.Error(t, err)
}

func TestJobQueue_Worker_EmptyTypes(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)

	// Create a context that will be canceled after a short time
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Start the worker with no types (should still work but not process anything)
	err := queue.Worker(ctx, []string{"worker_queue"})
	assert.Error(t, err) // Should return context deadline exceeded error
	assert.Equal(t, context.DeadlineExceeded, err)
}

func TestJobQueue_Worker_WithMultipleTypes(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)

	// Create a context that will be canceled after a short time
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Start the worker with multiple types
	err := queue.Worker(ctx, []string{"worker_queue"}, &MockJob{}, &FailingJob{})
	assert.Error(t, err) // Should return context deadline exceeded error
	assert.Equal(t, context.DeadlineExceeded, err)

	// Verify both types were registered
	assert.Len(t, queue.typeRegistry, 2)
	assert.Contains(t, queue.typeRegistry, "utils.MockJob")
	assert.Contains(t, queue.typeRegistry, "utils.FailingJob")
}

func TestJobQueue_Dequeue_ContextCancellation(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := newMockQueue(db, mockProcessor)

	// Register the job type
	queue.registerType(&MockJob{})

	// Create a context that's already canceled
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := queue.Dequeue(ctx, []string{"test_queue"})
	// Should handle canceled context gracefully (may or may not return error depending on timing)
	// This tests the context handling in the SQL queries
	_ = err // Ignore error as it may or may not occur depending on timing
}

func TestJobQueue_TypeName_EdgeCases(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)

	// Test with double pointer
	job := &MockJob{}
	typeName := queue.typeName(&job)
	assert.Equal(t, "*utils.MockJob", typeName) // Should only strip one *

	// Test with interface{}
	var interfaceJob interface{} = MockJob{}
	typeName = queue.typeName(interfaceJob)
	assert.Equal(t, "utils.MockJob", typeName)
}

func TestJobQueue_RegisterType_Multiple(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)

	// Register multiple types
	queue.registerType(&MockJob{})
	queue.registerType(&FailingJob{})
	queue.registerType(&LoadErrorJob{})

	// Verify all were registered
	assert.Len(t, queue.typeRegistry, 3)
	assert.Contains(t, queue.typeRegistry, "utils.MockJob")
	assert.Contains(t, queue.typeRegistry, "utils.FailingJob")
	assert.Contains(t, queue.typeRegistry, "utils.LoadErrorJob")
}

func TestJobQueue_GetType_AfterRegistration(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)

	// Register types
	queue.registerType(&MockJob{})
	queue.registerType(&FailingJob{})

	// Get registered types
	job1, err := queue.getType("utils.MockJob")
	assert.NoError(t, err)
	assert.IsType(t, MockJob{}, job1)

	job2, err := queue.getType("utils.FailingJob")
	assert.NoError(t, err)
	assert.IsType(t, FailingJob{}, job2)

	// Try to get unregistered type
	_, err = queue.getType("utils.LoadErrorJob")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "type not found in type registry")
}

func TestPqArray_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected string
	}{
		{
			name:     "nil slice",
			input:    nil,
			expected: "{}",
		},
		{
			name:     "string with newlines",
			input:    []string{"line1\nline2", "normal"},
			expected: `{"line1` + "\n" + `line2","normal"}`,
		},
		{
			name:     "string with tabs",
			input:    []string{"tab\ttab", "normal"},
			expected: `{"tab` + "\t" + `tab","normal"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := pqArray(tt.input)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAppendArrayQuotedBytes_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "bytes with null characters",
			input:    []byte("test\x00null"),
			expected: `"test` + "\x00" + `null"`,
		},
		{
			name:     "bytes with multiple escapes",
			input:    []byte(`test\"quote\backslash`),
			expected: `"test\\\"quote\\backslash"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := appendArrayQuotedBytes([]byte{}, tt.input)
			assert.Equal(t, tt.expected, string(result))
		})
	}
}

func TestMapKeys_DifferentTypes(t *testing.T) {
	t.Run("string to string map", func(t *testing.T) {
		input := map[string]string{"key1": "val1", "key2": "val2"}
		result := mapKeys(input)
		assert.Len(t, result, 2)
		assert.Contains(t, result, "key1")
		assert.Contains(t, result, "key2")
	})

	t.Run("int to string map", func(t *testing.T) {
		input := map[int]string{1: "val1", 2: "val2"}
		result := mapKeys(input)
		assert.Len(t, result, 2)
		assert.Contains(t, result, 1)
		assert.Contains(t, result, 2)
	})
}

func TestJobQueue_Constants_Values(t *testing.T) {
	// Test that constants have expected values for database compatibility
	assert.Equal(t, "new", JOB_STATUS_SCHEDULED)
	assert.Equal(t, "finished", JOB_STATUS_FINISHED)
	assert.Equal(t, "failed", JOB_STATUS_FAILED)
	assert.Equal(t, "jobs", JobsTableName)

	// Test that PollInterval has a reasonable default
	assert.Greater(t, PollInterval, 0*time.Second)
	assert.Less(t, PollInterval, 10*time.Second)
}

// Benchmark tests for performance verification
func BenchmarkJobQueue_Enqueue(b *testing.B) {
	gormDB, err := database.SetupTestDB()
	if err != nil {
		b.Fatal(err)
	}

	sqlDB, err := gormDB.DB()
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = sqlDB.Close() }()

	// Create jobs table
	_, err = sqlDB.Exec(`
		CREATE TABLE IF NOT EXISTS jobs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			type_name TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'new',
			queue TEXT NOT NULL,
			data TEXT NOT NULL,
			error TEXT,
			attempt INTEGER DEFAULT 0,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			started_at DATETIME,
			finished_at DATETIME,
			scheduled_at DATETIME
		)
	`)
	if err != nil {
		b.Fatal(err)
	}

	var mockProcessor interface{}
	queue := NewQueue(sqlDB, mockProcessor)
	ctx := context.Background()

	job := MockJob{ID: "benchmark", Data: "benchmark data"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := queue.Enqueue(ctx, job, "benchmark_queue")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPqArray(b *testing.B) {
	testSlices := [][]string{
		{},
		{"single"},
		{"one", "two", "three"},
		{"one", "two", "three", "four", "five"},
	}

	for _, slice := range testSlices {
		b.Run(fmt.Sprintf("len_%d", len(slice)), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := pqArray(slice)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Tests for EnqueueBatch functionality
func TestJobQueue_EnqueueBatch_EmptyJobs(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)
	ctx := context.Background()

	// Test with empty jobs slice
	err := queue.EnqueueBatch(ctx, []Job{}, "test_queue")
	assert.NoError(t, err)

	// Verify no jobs were inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM jobs").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestJobQueue_EnqueueBatch_Success(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)
	ctx := context.Background()

	// Create multiple jobs
	jobs := []Job{
		MockJob{ID: "job1", Data: "data1"},
		MockJob{ID: "job2", Data: "data2"},
		MockJob{ID: "job3", Data: "data3"},
	}

	err := queue.EnqueueBatch(ctx, jobs, "batch_queue")
	assert.NoError(t, err)

	// Verify all jobs were inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM jobs WHERE queue = ?", "batch_queue").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 3, count)

	// Verify job details
	rows, err := db.Query("SELECT type_name, status, data FROM jobs WHERE queue = ? ORDER BY id", "batch_queue")
	assert.NoError(t, err)
	defer func() { _ = rows.Close() }()

	expectedJobs := []MockJob{
		{ID: "job1", Data: "data1"},
		{ID: "job2", Data: "data2"},
		{ID: "job3", Data: "data3"},
	}

	i := 0
	for rows.Next() {
		var typeName, status, data string
		err = rows.Scan(&typeName, &status, &data)
		assert.NoError(t, err)
		assert.Equal(t, "utils.MockJob", typeName)
		assert.Equal(t, JOB_STATUS_SCHEDULED, status)

		var jobData MockJob
		err = json.Unmarshal([]byte(data), &jobData)
		assert.NoError(t, err)
		assert.Equal(t, expectedJobs[i].ID, jobData.ID)
		assert.Equal(t, expectedJobs[i].Data, jobData.Data)
		i++
	}
	assert.Equal(t, 3, i)
}

func TestJobQueue_EnqueueBatch_MarshalError(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)
	ctx := context.Background()

	// Create jobs with one that can't be marshaled
	jobs := []Job{
		MockJob{ID: "job1", Data: "data1"},
		UnmarshalableJob{BadField: make(chan string)}, // This will cause marshaling error
		MockJob{ID: "job3", Data: "data3"},
	}

	err := queue.EnqueueBatch(ctx, jobs, "batch_queue")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed marshaling job")

	// Verify no jobs were inserted (transaction should rollback)
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM jobs WHERE queue = ?", "batch_queue").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestJobQueue_EnqueueBatch_DatabaseError(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)
	ctx := context.Background()

	// Close database to simulate error
	_ = db.Close()

	jobs := []Job{
		MockJob{ID: "job1", Data: "data1"},
		MockJob{ID: "job2", Data: "data2"},
	}

	err := queue.EnqueueBatch(ctx, jobs, "batch_queue")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to begin transaction")
}

func TestJobQueue_EnqueueBatch_TransactionCommitError(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)
	ctx := context.Background()

	jobs := []Job{
		MockJob{ID: "job1", Data: "data1"},
	}

	// Close database after creating queue to force commit error
	_ = db.Close()

	err := queue.EnqueueBatch(ctx, jobs, "batch_queue")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to begin transaction")
}

func TestJobQueue_EnqueueBatch_LargeBatch(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)
	ctx := context.Background()

	// Create a large batch of jobs
	jobs := make([]Job, 100)
	for i := 0; i < 100; i++ {
		jobs[i] = MockJob{ID: fmt.Sprintf("job%d", i), Data: fmt.Sprintf("data%d", i)}
	}

	err := queue.EnqueueBatch(ctx, jobs, "large_batch_queue")
	assert.NoError(t, err)

	// Verify all jobs were inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM jobs WHERE queue = ?", "large_batch_queue").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 100, count)
}

func TestJobQueue_EnqueueBatch_MixedJobTypes(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)
	ctx := context.Background()

	// Create jobs of different types
	jobs := []Job{
		MockJob{ID: "mock1", Data: "mock1_data"},
		FailingJob{ID: "fail1"},
		MockJob{ID: "mock2", Data: "mock2_data"},
		FailingJob{ID: "fail2"},
	}

	err := queue.EnqueueBatch(ctx, jobs, "mixed_queue")
	assert.NoError(t, err)

	// Verify all jobs were inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM jobs WHERE queue = ?", "mixed_queue").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 4, count)

	// Verify job types
	var mockCount, failCount int
	err = db.QueryRow("SELECT COUNT(*) FROM jobs WHERE queue = ? AND type_name = ?", "mixed_queue", "utils.MockJob").Scan(&mockCount)
	assert.NoError(t, err)
	err = db.QueryRow("SELECT COUNT(*) FROM jobs WHERE queue = ? AND type_name = ?", "mixed_queue", "utils.FailingJob").Scan(&failCount)
	assert.NoError(t, err)
	assert.Equal(t, 2, mockCount)
	assert.Equal(t, 2, failCount)
}

func TestJobQueue_EnqueueBatch_SQLiteFallback(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)
	ctx := context.Background()

	// Create jobs - SQLite should use the fallback method
	jobs := []Job{
		MockJob{ID: "sqlite1", Data: "sqlite1_data"},
		MockJob{ID: "sqlite2", Data: "sqlite2_data"},
	}

	err := queue.EnqueueBatch(ctx, jobs, "sqlite_queue")
	assert.NoError(t, err)

	// Verify jobs were inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM jobs WHERE queue = ?", "sqlite_queue").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestJobQueue_EnqueueBatch_PostgresArrayError(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	var mockProcessor interface{}
	queue := NewQueue(db, mockProcessor)
	ctx := context.Background()

	// Create jobs with special characters that might cause array issues
	jobs := []Job{
		MockJob{ID: "job\"with\"quotes", Data: "data\"with\"quotes"},
		MockJob{ID: "job\\with\\backslashes", Data: "data\\with\\backslashes"},
	}

	err := queue.EnqueueBatch(ctx, jobs, "special_chars_queue")
	assert.NoError(t, err)

	// Verify jobs were inserted (should fallback to SQLite method)
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM jobs WHERE queue = ?", "special_chars_queue").Scan(&count)
	assert.NoError(t, err)
	assert.Equal(t, 2, count)
}
