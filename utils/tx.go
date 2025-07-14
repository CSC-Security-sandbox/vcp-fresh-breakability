package utils

import (
	"context"

	"gorm.io/gorm"
)

type contextKey string

const txKey contextKey = "gormTx"

// WithTx returns a new context with the gorm.DB transaction attached.
func WithTx(ctx context.Context, tx *gorm.DB) context.Context {
	return context.WithValue(ctx, txKey, tx)
}

// TxFromContext retrieves the gorm.DB transaction from the context, if present.
func TxFromContext(ctx context.Context) *gorm.DB {
	tx, _ := ctx.Value(txKey).(*gorm.DB)
	return tx
}
