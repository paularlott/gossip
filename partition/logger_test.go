package partition

import (
	"fmt"
	"strings"
	"testing"

	"github.com/paularlott/logger"
)

// tLogger writes the package's log lines to the test log (go test -v shows them).
type tLogger struct {
	t      *testing.T
	prefix string
}

func (l tLogger) log(level, msg string, kv []any) {
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s%s", level, l.prefix, msg)
	for i := 0; i+1 < len(kv); i += 2 {
		fmt.Fprintf(&b, " %v=%v", kv[i], kv[i+1])
	}
	l.t.Log(b.String())
}

func (l tLogger) Trace(msg string, kv ...any) {}
func (l tLogger) Debug(msg string, kv ...any) { l.log("DEBUG", msg, kv) }
func (l tLogger) Info(msg string, kv ...any)  { l.log("INFO", msg, kv) }
func (l tLogger) Warn(msg string, kv ...any)  { l.log("WARN", msg, kv) }
func (l tLogger) Error(msg string, kv ...any) { l.log("ERROR", msg, kv) }
func (l tLogger) Fatal(msg string, kv ...any) { l.log("FATAL", msg, kv) }
func (l tLogger) With(key string, value any) logger.Logger {
	return tLogger{l.t, l.prefix + fmt.Sprintf("%s=%v ", key, value)}
}
func (l tLogger) WithError(err error) logger.Logger { return l.With("error", err) }
func (l tLogger) WithGroup(group string) logger.Logger {
	return tLogger{l.t, l.prefix + group + ": "}
}

// testLogger is a logger for tests: silent unless the test runs with -v.
func testLogger(t *testing.T) logger.Logger {
	if testing.Verbose() {
		return tLogger{t: t}
	}
	return logger.NewNullLogger()
}
