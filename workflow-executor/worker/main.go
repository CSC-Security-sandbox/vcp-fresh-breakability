package main

import (
	"log"

	executor "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow-executor"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	// The client and worker are heavyweight objects that should be created once per process.
	c, err := client.Dial(client.Options{
		HostPort: client.DefaultHostPort,
	})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	w := worker.New(c, "JobsQueue", worker.Options{})

	w.RegisterWorkflow(executor.JobWorkflow)
	w.RegisterActivity(&executor.Jobs{})

	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("Unable to start worker", err)
	}
}
