package common

import (
	"io"
	"os"

	"github.com/paularlott/logger"
	logslog "github.com/paularlott/logger/slog"
)

var defaultLogger logger.Logger

func init() {
	// Initialize with default configuration
	defaultLogger = logslog.New(logslog.Config{
		Level:  "info",
		Format: "console",
		Writer: os.Stdout,
	})
}

// Configure sets up the logger with the given settings
func Configure(level, format string, writer io.Writer) {
	if writer == nil {
		writer = os.Stdout
	}

	defaultLogger = logslog.New(logslog.Config{
		Level:  level,
		Format: format,
		Writer: writer,
	})
}

// GetLogger returns the configured logger instance
func GetLogger() logger.Logger {
	return defaultLogger
}

// Package-level convenience functions
func Trace(msg string, keysAndValues ...any) {
	defaultLogger.Trace(msg, keysAndValues...)
}

func Debug(msg string, keysAndValues ...any) {
	defaultLogger.Debug(msg, keysAndValues...)
}

func Info(msg string, keysAndValues ...any) {
	defaultLogger.Info(msg, keysAndValues...)
}

func Warn(msg string, keysAndValues ...any) {
	defaultLogger.Warn(msg, keysAndValues...)
}

func Error(msg string, keysAndValues ...any) {
	defaultLogger.Error(msg, keysAndValues...)
}

func Fatal(msg string, keysAndValues ...any) {
	defaultLogger.Fatal(msg, keysAndValues...)
}

func With(key string, value any) logger.Logger {
	return defaultLogger.With(key, value)
}

func WithError(err error) logger.Logger {
	return defaultLogger.WithError(err)
}

func WithGroup(group string) logger.Logger {
	return defaultLogger.WithGroup(group)
}
