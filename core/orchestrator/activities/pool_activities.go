package activities

import (
	"fmt"
)

type PoolActivity struct {
}

// demo activities needs to be replaced by actual pool create steps
func (j *PoolActivity) CreatePoolActivity1() error {
	fmt.Println("Creating Vsa Cluster 1")
	return nil
}

func (j *PoolActivity) CreatePoolActivity2() error {
	fmt.Println("Creating Vsa Cluster 1")
	return nil
}

func (j *PoolActivity) CreatePoolActivity3() error {
	fmt.Println("Creating Vsa Cluster 2")
	return nil
}
