package logger

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/gorm/logger"
)

func TestLogMode(t *testing.T) {
	mockLogger := &log.MockLogger{}
	gsl := NewGormLogger(mockLogger, logger.Info)
	newLogger := gsl.LogMode(logger.Warn)
	assert.NotEqual(t, gsl, newLogger)
}

func TestInfo_Warn_Error(t *testing.T) {
	mockLogger := &log.MockLogger{}
	ctx := context.Background()
	msg := "test message"
	fields := log.Fields{"foo": "bar"}
	ctxWithFields := context.WithValue(ctx, middleware.TemporalSLoggerKey, fields)

	// Info
	mockLogger.On("WithFields", "requestFields", fields).Return(mockLogger)
	mockLogger.On("InfoContext", ctxWithFields, msg, mock.Anything).Return()
	gsl := NewGormLogger(mockLogger, logger.Info)
	gsl.Info(ctxWithFields, msg)
	mockLogger.AssertCalled(t, "WithFields", "requestFields", fields)
	mockLogger.AssertCalled(t, "InfoContext", ctxWithFields, msg, mock.Anything)

	// Warn
	mockLogger.On("WithFields", "requestFields", fields).Return(mockLogger)
	mockLogger.On("WarnContext", ctxWithFields, msg, mock.Anything).Return()
	gsl = NewGormLogger(mockLogger, logger.Warn)
	gsl.Warn(ctxWithFields, msg)
	mockLogger.AssertCalled(t, "WithFields", "requestFields", fields)
	mockLogger.AssertCalled(t, "WarnContext", ctxWithFields, msg, mock.Anything)

	// Error
	mockLogger.On("WithFields", "requestFields", fields).Return(mockLogger)
	mockLogger.On("ErrorContext", ctxWithFields, msg, mock.Anything).Return()
	gsl = NewGormLogger(mockLogger, logger.Error)
	gsl.Error(ctxWithFields, msg)
	mockLogger.AssertCalled(t, "WithFields", "requestFields", fields)
	mockLogger.AssertCalled(t, "ErrorContext", ctxWithFields, msg, mock.Anything)
}

func TestTrace_Error(t *testing.T) {
	mockLogger := &log.MockLogger{}
	ctx := context.Background()
	fields := log.Fields{"foo": "bar"}
	ctxWithFields := context.WithValue(ctx, middleware.TemporalSLoggerKey, fields)
	msg := "Database error"
	err := errors.New("db error")
	fc := func() (string, int64) { return "SELECT 1", 1 }

	mockLogger.On("With", mock.Anything).Return(mockLogger)
	mockLogger.On("ErrorContext", ctxWithFields, msg).Return()
	gsl := NewGormLogger(mockLogger, logger.Error)
	gsl.Trace(ctxWithFields, time.Now().Add(-time.Second), fc, err)
	mockLogger.AssertCalled(t, "With", mock.Anything)
	mockLogger.AssertCalled(t, "ErrorContext", ctxWithFields, msg)
}

func TestTrace_SlowQuery(t *testing.T) {
	mockLogger := &log.MockLogger{}
	ctx := context.Background()
	fields := log.Fields{"foo": "bar"}
	ctxWithFields := context.WithValue(ctx, middleware.TemporalSLoggerKey, fields)
	msg := "Slow query"
	fc := func() (string, int64) { return "SELECT 1", 1 }

	mockLogger.On("With", mock.Anything).Return(mockLogger)
	mockLogger.On("WarnContext", ctxWithFields, msg).Return()
	gsl := NewGormLogger(mockLogger, logger.Warn)
	gsl.Trace(ctxWithFields, time.Now().Add(-time.Second), fc, nil)
	mockLogger.AssertCalled(t, "With", mock.Anything)
	mockLogger.AssertCalled(t, "WarnContext", ctxWithFields, msg)
}

func TestTrace_Info(t *testing.T) {
	mockLogger := &log.MockLogger{}
	ctx := context.Background()
	fields := log.Fields{"foo": "bar"}
	ctxWithFields := context.WithValue(ctx, middleware.TemporalSLoggerKey, fields)
	msg := "Query"
	fc := func() (string, int64) { return "SELECT 1", 1 }

	mockLogger.On("With", mock.Anything).Return(mockLogger)
	mockLogger.On("DebugContext", ctxWithFields, msg).Return()
	gsl := NewGormLogger(mockLogger, logger.Info)
	gsl.Trace(ctxWithFields, time.Now(), fc, nil)
	mockLogger.AssertCalled(t, "With", mock.Anything)
	mockLogger.AssertCalled(t, "DebugContext", ctxWithFields, msg)
}

func TestTrace_Silent(t *testing.T) {
	mockLogger := &log.MockLogger{}
	ctx := context.Background()
	gsl := NewGormLogger(mockLogger, logger.Silent)
	fc := func() (string, int64) { return "SELECT 1", 1 }
	// Should not log anything
	gsl.Trace(ctx, time.Now(), fc, nil)
}

func TestInfo_Warn_Error_NoFields(t *testing.T) {
	mockLogger := &log.MockLogger{}
	ctx := context.Background()
	msg := "test message"
	// Info
	mockLogger.On("InfoContext", ctx, msg, mock.Anything).Return()
	gsl := NewGormLogger(mockLogger, logger.Info)
	gsl.Info(ctx, msg)
	mockLogger.AssertCalled(t, "InfoContext", ctx, msg, mock.Anything)
	// Warn
	mockLogger.On("WarnContext", ctx, msg, mock.Anything).Return()
	gsl = NewGormLogger(mockLogger, logger.Warn)
	gsl.Warn(ctx, msg)
	mockLogger.AssertCalled(t, "WarnContext", ctx, msg, mock.Anything)
	// Error
	mockLogger.On("ErrorContext", ctx, msg, mock.Anything).Return()
	gsl = NewGormLogger(mockLogger, logger.Error)
	gsl.Error(ctx, msg)
	mockLogger.AssertCalled(t, "ErrorContext", ctx, msg, mock.Anything)
}

func TestInfo_Warn_Error_BelowThreshold(t *testing.T) {
	mockLogger := &log.MockLogger{}
	ctx := context.Background()
	msg := "test message"
	gsl := NewGormLogger(mockLogger, logger.Warn)
	gsl.Info(ctx, msg) // Should not log
	gsl = NewGormLogger(mockLogger, logger.Error)
	gsl.Warn(ctx, msg) // Should not log
	gsl = NewGormLogger(mockLogger, logger.Silent)
	gsl.Error(ctx, msg) // Should not log
}

func TestInfo_Warn_Error_WithData(t *testing.T) {
	mockLogger := &log.MockLogger{}
	ctx := context.Background()
	msg := "test message"
	fields := log.Fields{"foo": "bar"}
	ctxWithFields := context.WithValue(ctx, middleware.TemporalSLoggerKey, fields)
	data := []interface{}{1, "two"}
	mockLogger.On("WithFields", "requestFields", fields).Return(mockLogger)
	mockLogger.On("InfoContext", ctxWithFields, msg, 1, "two").Return()
	gsl := NewGormLogger(mockLogger, logger.Info)
	gsl.Info(ctxWithFields, msg, data...)
	mockLogger.AssertCalled(t, "InfoContext", ctxWithFields, msg, 1, "two")
}

func TestTrace_Error_NoFields(t *testing.T) {
	mockLogger := &log.MockLogger{}
	ctx := context.Background()
	msg := "Database error"
	err := errors.New("db error")
	fc := func() (string, int64) { return "SELECT 1", 1 }
	mockLogger.On("With", mock.Anything).Return(mockLogger)
	mockLogger.On("ErrorContext", ctx, msg).Return()
	gsl := NewGormLogger(mockLogger, logger.Error)
	gsl.Trace(ctx, time.Now().Add(-time.Second), fc, err)
	mockLogger.AssertCalled(t, "With", mock.Anything)
	mockLogger.AssertCalled(t, "ErrorContext", ctx, msg)
}

func TestTrace_SlowQuery_NoFields(t *testing.T) {
	mockLogger := &log.MockLogger{}
	ctx := context.Background()
	msg := "Slow query"
	fc := func() (string, int64) { return "SELECT 1", 1 }
	mockLogger.On("With", mock.Anything).Return(mockLogger)
	mockLogger.On("WarnContext", ctx, msg).Return()
	gsl := NewGormLogger(mockLogger, logger.Warn)
	gsl.Trace(ctx, time.Now().Add(-time.Second), fc, nil)
	mockLogger.AssertCalled(t, "With", mock.Anything)
	mockLogger.AssertCalled(t, "WarnContext", ctx, msg)
}

func TestTrace_Info_NoFields(t *testing.T) {
	mockLogger := &log.MockLogger{}
	ctx := context.Background()
	msg := "Query"
	fc := func() (string, int64) { return "SELECT 1", 1 }
	mockLogger.On("With", mock.Anything).Return(mockLogger)
	mockLogger.On("DebugContext", ctx, msg).Return()
	gsl := NewGormLogger(mockLogger, logger.Info)
	gsl.Trace(ctx, time.Now(), fc, nil)
	mockLogger.AssertCalled(t, "With", mock.Anything)
	mockLogger.AssertCalled(t, "DebugContext", ctx, msg)
}

func TestTrace_Silent_NoLog(t *testing.T) {
	mockLogger := &log.MockLogger{}
	ctx := context.Background()
	fc := func() (string, int64) { return "SELECT 1", 1 }
	gsl := NewGormLogger(mockLogger, logger.Silent)
	gsl.Trace(ctx, time.Now(), fc, nil) // Should not log
}

func TestTrace_SlowThresholdZero(t *testing.T) {
	mockLogger := &log.MockLogger{}
	ctx := context.Background()
	fields := log.Fields{"foo": "bar"}
	ctxWithFields := context.WithValue(ctx, middleware.TemporalSLoggerKey, fields)
	fc := func() (string, int64) { return "SELECT 1", 1 }
	gsl := NewGormLogger(mockLogger, logger.Warn)
	gsl.(*GormSlogLogger).config.SlowThreshold = 0
	// Should not log slow query
	gsl.Trace(ctxWithFields, time.Now().Add(-time.Second), fc, nil)
}

func TestTrace_FutureBegin(t *testing.T) {
	mockLogger := &log.MockLogger{}
	ctx := context.Background()
	fields := log.Fields{"foo": "bar"}
	ctxWithFields := context.WithValue(ctx, middleware.TemporalSLoggerKey, fields)
	fc := func() (string, int64) { return "SELECT 1", 1 }
	mockLogger.On("With", mock.Anything).Return(mockLogger)
	mockLogger.On("DebugContext", ctxWithFields, "Query").Return()
	gsl := NewGormLogger(mockLogger, logger.Info)
	// begin in future, elapsed negative
	gsl.Trace(ctxWithFields, time.Now().Add(time.Second), fc, nil)
	mockLogger.AssertCalled(t, "With", mock.Anything)
	mockLogger.AssertCalled(t, "DebugContext", ctxWithFields, "Query")
}

func TestTrace_FcReturnsDifferentValues(t *testing.T) {
	mockLogger := &log.MockLogger{}
	ctx := context.Background()
	fields := log.Fields{"foo": "bar"}
	ctxWithFields := context.WithValue(ctx, middleware.TemporalSLoggerKey, fields)
	fc := func() (string, int64) { return "DIFFERENT SQL", 42 }
	mockLogger.On("With", mock.Anything).Return(mockLogger)
	mockLogger.On("DebugContext", ctxWithFields, "Query").Return()
	gsl := NewGormLogger(mockLogger, logger.Info)
	gsl.Trace(ctxWithFields, time.Now(), fc, nil)
	mockLogger.AssertCalled(t, "With", mock.Anything)
	mockLogger.AssertCalled(t, "DebugContext", ctxWithFields, "Query")
}

func TestLogMode_AllLevels(t *testing.T) {
	mockLogger := &log.MockLogger{}
	gsl := NewGormLogger(mockLogger, logger.Info)
	for _, level := range []logger.LogLevel{logger.Silent, logger.Error, logger.Warn, logger.Info} {
		newLogger := gsl.LogMode(level)
		assert.NotNil(t, newLogger)
	}
}

func TestNewGormLogger_CustomConfig(t *testing.T) {
	mockLogger := &log.MockLogger{}
	gsl := NewGormLogger(mockLogger, logger.Info)
	assert.Equal(t, 200*time.Millisecond, gsl.(*GormSlogLogger).config.SlowThreshold)
	assert.True(t, gsl.(*GormSlogLogger).config.IgnoreRecordNotFoundError)
	assert.False(t, gsl.(*GormSlogLogger).config.Colorful)
}

func TestInfo_Warn_Error_NilContext(t *testing.T) {
	mockLogger := &log.MockLogger{}
	msg := "test message"
	ctx := context.TODO() // Use context.TODO() instead of nil
	mockLogger.On("InfoContext", ctx, msg, mock.Anything).Return()
	gsl := NewGormLogger(mockLogger, logger.Info)
	gsl.Info(ctx, msg)
	mockLogger.AssertCalled(t, "InfoContext", ctx, msg, mock.Anything)
}

func TestTrace_NilFc(t *testing.T) {
	mockLogger := &log.MockLogger{}
	ctx := context.Background()
	gsl := NewGormLogger(mockLogger, logger.Info)
	// Should not panic if fc is nil - should handle gracefully
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Trace panicked with nil fc: %v", r)
		}
	}()
	gsl.Trace(ctx, time.Now(), nil, nil)
}
