package utils

import (
	"gorm.io/gorm"
)

type Pagination struct {
	Limit  int
	Offset int
}

func Paginate(pagination *Pagination) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if pagination == nil {
			return db
		}
		limit := pagination.Limit
		if limit == 0 {
			// TODO: Need to configure using ENV
			limit = 1000 // default limit
		}
		offset := pagination.Offset
		return db.Offset(offset).Limit(limit)
	}
}
