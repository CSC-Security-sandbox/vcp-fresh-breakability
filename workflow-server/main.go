package main

import (
	"log"

	"go.temporal.io/server/temporal"
)

func main() {
	s, err := temporal.NewServer(
		temporal.ForServices(temporal.DefaultServices),
		temporal.InterruptOn(temporal.InterruptCh()),
	)
	if err != nil {
		log.Fatal(err)
	}
	err = s.Start()
	if err != nil {
		log.Fatal(err)
	}
}
