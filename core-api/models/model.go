package models

// Snapshot represents a single snapshot resource
type Snapshot struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Time string `json:"time"`
}

// Volume represents a single volume resource
type Volume struct {
	ID        string     `json:"id"`
	Name      string     `json:"name"`
	Size      int        `json:"size"`
	Snapshots []Snapshot `json:"snapshots"`
}

// SVM represents a single SVM resource
type SVM struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`
	Volumes []Volume `json:"volumes"`
}

// Pool represents a single pool resource
type Pool struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	SVMs []SVM  `json:"svms"`
}
