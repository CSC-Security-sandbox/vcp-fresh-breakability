package utils

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

func TestWithTxAndTxFromContext(t *testing.T) {
	ctx := context.Background()
	db := &gorm.DB{} // Use a real *gorm.DB pointer for type safety

	ctxWithTx := WithTx(ctx, db)
	got := TxFromContext(ctxWithTx)
	assert.Equal(t, db, got, "TxFromContext should return the same *gorm.DB passed to WithTx")
}

func TestTxFromContext_NoTx(t *testing.T) {
	ctx := context.Background()
	got := TxFromContext(ctx)
	assert.Nil(t, got, "TxFromContext should return nil if no transaction is present in context")
}

func TestWithTx_Overwrite(t *testing.T) {
	ctx := context.Background()
	db1 := &gorm.DB{}
	db2 := &gorm.DB{}
	ctxWithTx1 := WithTx(ctx, db1)
	ctxWithTx2 := WithTx(ctxWithTx1, db2)
	got := TxFromContext(ctxWithTx2)
	assert.Equal(t, db2, got, "WithTx should overwrite previous transaction in context")
}
