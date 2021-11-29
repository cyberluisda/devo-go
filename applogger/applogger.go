/*
Package applogger define a set of simple logger interfaces that can be implement by the libraries client
and that it should be used in all devo-go libraries

Some implementations are added here like NoLogAppLogger (no log logger) and WriterAppLogger log that uses
a provided Writer as destination of messages.
*/
package applogger

import (
	"fmt"
	"io"
	"strings"
	"time"
)

// Level defines the level when send a logger trace
type Level int

const (
	// FATAL is the fatal level
	FATAL Level = iota
	// ERROR is the error level
	ERROR
	// WARNING is the warning level
	WARNING
	// INFO is the info level
	INFO
	// DEBUG is the debug Level
	DEBUG
)

// LevelString returns the string value of the Level l
func LevelString(l Level) string {
	lvlStr := ""
	switch l {
	case DEBUG:
		lvlStr = "DEBUG"
	case INFO:
		lvlStr = "INFO"
	case WARNING:
		lvlStr = "WARN"
	case ERROR:
		lvlStr = "ERROR"
	case FATAL:
		lvlStr = "FATAL"
	}

	return lvlStr
}

//LevelComp returns: 0 if l is equal to o, 1 if l is greater than o or -1 if l is less than o
func LevelComp(l, o Level) int {
	if l < o {
		return -1
	}
	if l > o {
		return 1
	}

	return 0
}

// SimpleAppLogger defines the minium AppLogger interface
type SimpleAppLogger interface {
	IsLevelEnabled(l Level) bool
	Log(l Level, a ...interface{})
	Logf(l Level, format string, a ...interface{})
}

// AppLogger defines the Logger interface
type AppLogger interface {
	SimpleAppLogger
	Debug(a ...interface{})
	Debugf(format string, a ...interface{})
	Info(a ...interface{})
	Infof(format string, a ...interface{})
	Warn(a ...interface{})
	Warnf(format string, a ...interface{})
	Error(a ...interface{})
	Errorf(format string, a ...interface{})
	Fatal(a ...interface{})
	Fatalf(format string, a ...interface{})
}

// NoLogAppLogger is a dummy implementation of AppLogger that does not do nothing
type NoLogAppLogger struct{}

// IsLevelEnabled is the implementation of IsLevelEnabled in AppLogger interface for NoLogAppLogger
func (*NoLogAppLogger) IsLevelEnabled(l Level) bool {
	return false
}

// Debug is the implementation of Debug in AppLogger interface for NoLogAppLogger
func (*NoLogAppLogger) Debug(a ...interface{}) {
}

// Debugf is the implementation of Debugf in AppLogger interface for NoLogAppLogger
func (*NoLogAppLogger) Debugf(format string, a ...interface{}) {
}

// Info is the implementation of Info in AppLogger interface for NoLogAppLogger
func (*NoLogAppLogger) Info(a ...interface{}) {
}

// Infof is the implementation of Infof in AppLogger interface for NoLogAppLogger
func (*NoLogAppLogger) Infof(format string, a ...interface{}) {
}

// Warn is the implementation of Warn in AppLogger interface for NoLogAppLogger
func (*NoLogAppLogger) Warn(a ...interface{}) {
}

// Warnf is the implementation of Warnf in AppLogger interface for NoLogAppLogger
func (*NoLogAppLogger) Warnf(format string, a ...interface{}) {
}

// Error is the implementation of Error in AppLogger interface for NoLogAppLogger
func (*NoLogAppLogger) Error(a ...interface{}) {
}

// Errorf is the implementation of Errorf in AppLogger interface for NoLogAppLogger
func (*NoLogAppLogger) Errorf(format string, a ...interface{}) {
}

// Fatal is the implementation of Fatal in AppLogger interface for NoLogAppLogger
func (*NoLogAppLogger) Fatal(a ...interface{}) {
}

// Fatalf is the implementation of Fatalf in AppLogger interface for NoLogAppLogger
func (*NoLogAppLogger) Fatalf(format string, a ...interface{}) {
}

// Log is the implementation of Log in AppLogger interface for NoLogAppLogger
func (*NoLogAppLogger) Log(l Level, a ...interface{}) {
}

// Logf is the implementation of Logf in AppLogger interface for NoLogAppLogger
func (*NoLogAppLogger) Logf(l Level, format string, a ...interface{}) {
}

// WriterAppLogger is an implementation of AppLogger that writes messages to io.Writer
type WriterAppLogger struct {
	Writer     io.Writer
	Level      Level
	AddNow     bool
	TimeLayout string
}

// IsLevelEnabled is the implementation of IsLevelEnabled in AppLogger interface for WriterAppLogger
func (w *WriterAppLogger) IsLevelEnabled(l Level) bool {
	return LevelComp(l, w.Level) <= 0
}

// Debug is the implementation of Debug in AppLogger interface for WriterAppLogger
func (w *WriterAppLogger) Debug(a ...interface{}) {
	w.Log(DEBUG, a...)
}

// Debugf is the implementation of Debugf in AppLogger interface for WriterAppLogger
func (w *WriterAppLogger) Debugf(format string, a ...interface{}) {
	w.Logf(DEBUG, format, a...)
}

// Info is the implementation of Info in AppLogger interface for WriterAppLogger
func (w *WriterAppLogger) Info(a ...interface{}) {
	w.Log(INFO, a...)
}

// Infof is the implementation of Infof in AppLogger interface for WriterAppLogger
func (w *WriterAppLogger) Infof(format string, a ...interface{}) {
	w.Logf(INFO, format, a...)
}

// Warn is the implementation of Warn in AppLogger interface for WriterAppLogger
func (w *WriterAppLogger) Warn(a ...interface{}) {
	w.Log(WARNING, a...)
}

// Warnf is the implementation of Warnf in AppLogger interface for WriterAppLogger
func (w *WriterAppLogger) Warnf(format string, a ...interface{}) {
	w.Logf(WARNING, format, a...)
}

// Error is the implementation of Error in AppLogger interface for WriterAppLogger
func (w *WriterAppLogger) Error(a ...interface{}) {
	w.Log(ERROR, a...)
}

// Errorf is the implementation of Errorf in AppLogger interface for WriterAppLogger
func (w *WriterAppLogger) Errorf(format string, a ...interface{}) {
	w.Logf(ERROR, format, a...)
}

// Fatal is the implementation of Fatal in AppLogger interface for WriterAppLogger
func (w *WriterAppLogger) Fatal(a ...interface{}) {
	w.Log(FATAL, a...)
}

// Fatalf is the implementation of Fatalf in AppLogger interface for WriterAppLogger
func (w *WriterAppLogger) Fatalf(format string, a ...interface{}) {
	w.Logf(FATAL, format, a...)
}

// Log is the implementation of Log in AppLogger interface for WriterAppLogger
func (w *WriterAppLogger) Log(l Level, a ...interface{}) {
	if l > w.Level {
		return
	}
	offSet := 2
	if !w.AddNow {
		offSet = 1
	}

	params := make([]interface{}, len(a)+offSet)
	if w.AddNow {
		if w.TimeLayout != "" {
			params[0] = time.Now().Format(w.TimeLayout)
		} else {
			params[0] = time.Now().String()
		}
		params[1] = LevelString(l)
	} else {
		params[0] = LevelString(l)
	}

	for i := 0; i < len(a); i++ {
		j := i + offSet
		params[j] = a[i]
	}

	fmt.Fprintln(w.Writer, params...)
}

// Logf is the implementation of Logf in AppLogger interface for WriterAppLogger
func (w *WriterAppLogger) Logf(l Level, format string, a ...interface{}) {
	if l > w.Level {
		return
	}
	now := ""
	if w.AddNow {
		if w.TimeLayout != "" {
			now = time.Now().Format(w.TimeLayout)
		} else {
			now = time.Now().String()
		}
	}
	fixedFormat := fixFormat(l, format, now)
	fmt.Fprintf(w.Writer, fixedFormat, a...)
}

func fixFormat(l Level, s string, now string) string {
	sb := &strings.Builder{}
	if now != "" {
		sb.WriteString(now)
		sb.WriteString(" ")
	}
	sb.WriteString(LevelString(l))
	sb.WriteString(" ")
	sb.WriteString(s)

	if s[len(s)-1] != '\n' {
		sb.WriteRune('\n')
	}

	return sb.String()
}
