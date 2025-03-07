package main

import (
	"github.com/labstack/gommon/log"
	"go.temporal.io/server/temporal"
)

func main() {
	s, err := temporal.NewServer()
	if err != nil {
		log.Fatal(err)
	}
	err = s.Start()
	if err != nil {
		log.Fatal(err)
	}
}
