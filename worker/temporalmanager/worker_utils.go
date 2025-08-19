package temporalmanager

import (
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

type Worker struct {
	client client.Client
	worker worker.Worker
}

// returns new worker object
func NewWorker(c client.Client, taskQueue string) *Worker {
	w := worker.New(c, taskQueue, worker.Options{WorkflowPanicPolicy: worker.FailWorkflow})
	return &Worker{
		client: c,
		worker: w,
	}
}

// RegisterWorkflow registers a workflow with the worker
func (w *Worker) RegisterWorkflow(workflow interface{}) {
	w.worker.RegisterWorkflow(workflow)
}

// RegisterActivity registers an activity with the worker
func (w *Worker) RegisterActivity(activity interface{}) {
	w.worker.RegisterActivity(activity)
}

// Runs the worker
func (w *Worker) Run() error {
	return w.worker.Run(worker.InterruptCh())
}

// Stops the worker
func (w *Worker) Stop() {
	w.worker.Stop()
}

// Get current worker
func (w *Worker) GetWorker() worker.Worker {
	return w.worker
}

func (w *Worker) GetClient() client.Client {
	return w.client
}
