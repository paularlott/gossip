package gossip

// Logger is a generic logger interface similar to BadgerDB's logger
type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})

	Field(key string, value interface{}) Logger
	Err(err error) Logger
}

// NullLogger implements the Logger interface with no-op methods
type NullLogger struct{}

func NewNullLogger() *NullLogger {
	return &NullLogger{}
}

func (l *NullLogger) Debugf(format string, args ...interface{}) {}
func (l *NullLogger) Infof(format string, args ...interface{})  {}
func (l *NullLogger) Warnf(format string, args ...interface{})  {}
func (l *NullLogger) Errorf(format string, args ...interface{}) {}
func (l *NullLogger) Field(key string, value interface{}) Logger {
	return l
}
func (l *NullLogger) Err(err error) Logger {
	return l
}
