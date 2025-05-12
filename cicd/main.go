package cicd

import "main/cmd"

import (
	release_cmd "main/release-cmd"
)

func main() {
	cmd.Execute()
	release_cmd.Execute()
}
