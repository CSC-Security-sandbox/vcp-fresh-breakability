package main

import (
	"context"
	"log"

	"github.com/pborman/uuid"
	choice_multi "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow-executor"
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
		ID:        "multi_choice_" + uuid.New(),
		TaskQueue: "choice-multi",
	}

	we, err := c.ExecuteWorkflow(context.Background(), workflowOptions, choice_multi.MultiChoiceWorkflow1)
	if err != nil {
		log.Fatalln("Unable to execute workflow", err)
	}
	log.Println("Started workflow", "WorkflowID", we.GetID(), "RunID", we.GetRunID())

}
