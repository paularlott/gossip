package main

import (
	"github.com/paularlott/gossip"

	"github.com/rs/zerolog"
)

// ZerologLogger implements the Logger interface using zerolog
type ZerologLogger struct {
	zl zerolog.Logger
}

func NewZerologLogger(zl zerolog.Logger) *ZerologLogger {
	return &ZerologLogger{zl: zl}
}

func (l *ZerologLogger) Debugf(format string, args ...interface{}) {
	l.zl.Debug().Msgf(format, args...)
}
func (l *ZerologLogger) Infof(format string, args ...interface{}) {
	l.zl.Info().Msgf(format, args...)
}
func (l *ZerologLogger) Warnf(format string, args ...interface{}) {
	l.zl.Warn().Msgf(format, args...)
}
func (l *ZerologLogger) Errorf(format string, args ...interface{}) {
	l.zl.Error().Msgf(format, args...)
}
func (l *ZerologLogger) Field(key string, value interface{}) gossip.Logger {
	return &ZerologLogger{
		zl: l.zl.With().Interface(key, value).Logger(),
	}
}
func (l *ZerologLogger) Err(err error) gossip.Logger {
	return &ZerologLogger{
		zl: l.zl.With().Err(err).Logger(),
	}
}
