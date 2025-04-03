package datamodel

type Pool struct {
	UUID string `gorm:"type:varchar(36)"`
	Name string `gorm:"type:varchar(255)"`
	Size int64  `gorm:"type:bigint"`
}

type Volume struct {
	UUID string `gorm:"type:varchar(36)"`
}

type Svm struct {
	UUID string `gorm:"type:varchar(36)"`
}
