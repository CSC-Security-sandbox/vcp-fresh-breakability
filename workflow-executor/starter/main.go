package main

import (
	"context"
	"log"

	"github.com/pborman/uuid"
	executor "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow-executor"
	"go.temporal.io/sdk/client"
)

func main() {
	// The client is a heavyweight object that should be created once per process.
	c, err := client.Dial(client.Options{
		HostPort: client.DefaultHostPort,
	})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	workflowOptions := client.StartWorkflowOptions{
		ID:        "Job_" + uuid.New(),
		TaskQueue: "JobsQueue",
	}

	we, err := c.ExecuteWorkflow(context.Background(), workflowOptions, executor.JobWorkflow)
	if err != nil {
		log.Fatalln("Unable to execute workflow", err)
	}
	log.Println("Started workflow", "WorkflowID", we.GetID(), "RunID", we.GetRunID())

}
