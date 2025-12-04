package main

import "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"

type Config struct {
	AppPort string
}

func LoadConfig() *Config {
	return &Config{
		AppPort: env.GetString("PORT", "8080"),
	}
}
