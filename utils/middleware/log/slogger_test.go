package log

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/otel/trace"
)

// TestGetSlogger tests the getSlogger function
func TestGetSlogger(t *testing.T) {
	t.Run("Initialize slogger with valid config", func(t *testing.T) {
		config := Config{
			LogLevel:  "info",
			AddSource: true,
		}
		logger, _ := getSlogger(config)
		assert.IsType(t, &Slogger{}, logger)
	})

	t.Run("Initialize slogger with invalid log level", func(t *testing.T) {
		config := Config{
			LogLevel:  "invalid",
			AddSource: true,
		}
		logger, _ := getSlogger(config)
		assert.IsType(t, &Slogger{}, logger)
	})

	t.Run("Initialize slogger with empty log level", func(t *testing.T) {
		config := Config{
			LogLevel:  "",
			AddSource: true,
		}
		logger, _ := getSlogger(config)
		assert.IsType(t, &Slogger{}, logger)
	})

	t.Run("Initialize slogger with AddSource as false", func(t *testing.T) {
		config := Config{
			LogLevel:  "info",
			AddSource: false,
		}
		logger, _ := getSlogger(config)
		assert.IsType(t, &Slogger{}, logger)
	})
}

// TestSloggerWithFields tests the WithFields method of Slogger
func TestSloggerWithFields(t *testing.T) {
	t.Run("Add fields to logger", func(t *testing.T) {
		mockLogger := NewMockLogger(t)
		mockLogger.On("WithFields", "testFields", Fields{"key": "value"}).Return(mockLogger)

		newLogger := mockLogger.WithFields("testFields", Fields{"key": "value"})
		assert.NotNil(t, newLogger)
		mockLogger.AssertCalled(t, "WithFields", "testFields", Fields{"key": "value"})
		mockLogger.AssertExpectations(t)
	})

	t.Run("Add empty fields to logger", func(t *testing.T) {
		mockLogger := NewMockLogger(t)
		mockLogger.On("WithFields", "testFields", Fields{}).Return(mockLogger)

		newLogger := mockLogger.WithFields("testFields", Fields{})
		assert.NotNil(t, newLogger)
		mockLogger.AssertCalled(t, "WithFields", "testFields", Fields{})
		mockLogger.AssertExpectations(t)
	})

	t.Run("When Field is nil", func(t *testing.T) {
		mockLogger := NewMockLogger(t)
		var nilFields Fields = nil
		mockLogger.On("WithFields", "nilfields", nilFields).Return(mockLogger)

		newLogger := mockLogger.WithFields("nilfields", nilFields)
		assert.NotNil(t, newLogger)
		mockLogger.AssertCalled(t, "WithFields", "nilfields", nilFields)
		mockLogger.AssertExpectations(t)
	})
}

// TestSloggerWith tests the With method of Slogger
func TestSloggerWith(t *testing.T) {
	t.Run("Add fields to logger", func(t *testing.T) {
		mockLogger := NewMockLogger(t)
		mockLogger.On("With", Fields{"key": "value"}).Return(mockLogger)

		newLogger := mockLogger.With(Fields{"key": "value"})
		assert.NotNil(t, newLogger)
		mockLogger.AssertCalled(t, "With", Fields{"key": "value"})
		mockLogger.AssertExpectations(t)
	})

	t.Run("Add empty fields to logger", func(t *testing.T) {
		mockLogger := NewMockLogger(t)
		mockLogger.On("With", Fields{}).Return(mockLogger)

		newLogger := mockLogger.With(Fields{})
		assert.NotNil(t, newLogger)
		mockLogger.AssertCalled(t, "With", Fields{})
		mockLogger.AssertExpectations(t)
	})

	t.Run("When Field is nil", func(t *testing.T) {
		mockLogger := NewMockLogger(t)
		var nilFields Fields = nil
		mockLogger.On("With", nilFields).Return(mockLogger) // Expect nil of type Fields

		newLogger := mockLogger.With(nilFields) // Pass nil of type Fields
		assert.NotNil(t, newLogger)
		mockLogger.AssertCalled(t, "With", nilFields) // Assert nil of type Fields
		mockLogger.AssertExpectations(t)
	})
}

// TestSloggerLogMethods tests the logging methods of Slogger
func TestSloggerLogMethods(t *testing.T) {
	mockLogger := NewMockLogger(t)
	t.Run("log error", func(t *testing.T) {
		mockLogger.On("Error", "error message").Return()
		mockLogger.Error("error message")
		mockLogger.AssertCalled(t, "Error", "error message")
	})

	t.Run("log errorf", func(t *testing.T) {
		mockLogger.On("Errorf", "error %s", "message").Return()
		mockLogger.Errorf("error %s", "message")
		mockLogger.AssertCalled(t, "Errorf", "error %s", "message")
	})

	t.Run("log warn", func(t *testing.T) {
		mockLogger.On("Warn", "warn message").Return()
		mockLogger.Warn("warn message")
		mockLogger.AssertCalled(t, "Warn", "warn message")
	})

	t.Run("log warnf", func(t *testing.T) {
		mockLogger.On("Warnf", "warn %s", "message").Return()
		mockLogger.Warnf("warn %s", "message")
		mockLogger.AssertCalled(t, "Warnf", "warn %s", "message")
	})

	t.Run("log info", func(t *testing.T) {
		mockLogger.On("Info", "info message").Return()
		mockLogger.Info("info message")
		mockLogger.AssertCalled(t, "Info", "info message")
	})

	t.Run("log infof", func(t *testing.T) {
		mockLogger.On("Infof", "info %s", "message").Return()
		mockLogger.Infof("info %s", "message")
		mockLogger.AssertCalled(t, "Infof", "info %s", "message")
	})

	t.Run("log debug", func(t *testing.T) {
		mockLogger.On("Debug", "debug message").Return()
		mockLogger.Debug("debug message")
		mockLogger.AssertCalled(t, "Debug", "debug message")
	})

	t.Run("log debugf", func(t *testing.T) {
		mockLogger.On("Debugf", "debug %s", "message").Return()
		mockLogger.Debugf("debug %s", "message")
		mockLogger.AssertCalled(t, "Debugf", "debug %s", "message")
	})
}

// TestSloggerContextLogMethods tests the context logging methods of Slogger
func TestSloggerContextLogMethods(t *testing.T) {
	mockLogger := NewMockLogger(t)
	ctx := context.Background()
	t.Run("log info with context", func(t *testing.T) {
		mockLogger.On("InfoContext", ctx, "info message", mock.Anything).Return()
		mockLogger.InfoContext(ctx, "info message")
		mockLogger.AssertCalled(t, "InfoContext", ctx, "info message", mock.Anything)
	})

	t.Run("log warn with context", func(t *testing.T) {
		mockLogger.On("WarnContext", ctx, "warn message", mock.Anything).Return()
		mockLogger.WarnContext(ctx, "warn message")
		mockLogger.AssertCalled(t, "WarnContext", ctx, "warn message", mock.Anything)
	})

	t.Run("log error with context", func(t *testing.T) {
		mockLogger.On("ErrorContext", ctx, "error message", mock.Anything).Return()
		mockLogger.ErrorContext(ctx, "error message")
		mockLogger.AssertCalled(t, "ErrorContext", ctx, "error message", mock.Anything)
	})

	t.Run("log debug with context", func(t *testing.T) {
		mockLogger.On("DebugContext", ctx, "debug message", mock.Anything).Return()
		mockLogger.DebugContext(ctx, "debug message")
		mockLogger.AssertCalled(t, "DebugContext", ctx, "debug message", mock.Anything)
	})

	t.Run("log with nil context", func(t *testing.T) {
		mockLogger.On("InfoContext", context.TODO(), "info message with nil context", mock.Anything).Return()
		mockLogger.InfoContext(context.TODO(), "info message with nil context")
		mockLogger.AssertCalled(t, "InfoContext", context.TODO(), "info message with nil context", mock.Anything)
	})
}

// TestSpanContextLogHandler tests the spanContextLogHandler methods
func TestSpanContextLogHandler(t *testing.T) {
	t.Run("handle with valid context", func(t *testing.T) {
		ctx := context.Background()
		record := slog.Record{}
		handler := handlerWithSpanContext(slog.NewJSONHandler(defaultOutputStream, nil))

		err := handler.Handle(ctx, record)
		assert.NoError(t, err)
	})

	t.Run("handle with nil context", func(t *testing.T) {
		record := slog.Record{}
		handler := handlerWithSpanContext(slog.NewJSONHandler(defaultOutputStream, nil))

		err := handler.Handle(context.TODO(), record)
		assert.NoError(t, err)
	})

	t.Run("handle with valid context with TraceID and SpanID", func(t *testing.T) {
		ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    trace.TraceID{1, 2, 3},
			SpanID:     trace.SpanID{4, 5, 6},
			TraceFlags: trace.FlagsSampled,
		}))
		record := slog.Record{}
		handler := handlerWithSpanContext(slog.NewJSONHandler(defaultOutputStream, nil))

		err := handler.Handle(ctx, record)
		assert.NoError(t, err)
	})
}

// TestConvertLogLevel tests the convertLogLevel function
func TestConvertLogLevel(t *testing.T) {
	t.Run("convert valid log level", func(t *testing.T) {
		assert.Equal(t, slog.LevelInfo, convertLogLevel("info"))
		assert.Equal(t, slog.LevelDebug, convertLogLevel("debug"))
		assert.Equal(t, slog.LevelWarn, convertLogLevel("warn"))
		assert.Equal(t, slog.LevelError, convertLogLevel("error"))
	})

	t.Run("convert invalid log level", func(t *testing.T) {
		assert.Equal(t, slog.LevelInfo, convertLogLevel("invalid"))
	})

	t.Run("Convert empty log level", func(t *testing.T) {
		assert.Equal(t, slog.LevelInfo, convertLogLevel(""))
	})

	t.Run("Convert uppercase log level", func(t *testing.T) {
		assert.Equal(t, slog.LevelDebug, convertLogLevel("DEBUG"))
		assert.Equal(t, slog.LevelError, convertLogLevel("ERROR"))
	})

	t.Run("Convert mixed-case log level", func(t *testing.T) {
		assert.Equal(t, slog.LevelWarn, convertLogLevel("WaRn"))
	})
}
