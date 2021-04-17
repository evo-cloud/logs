package logs

import (
	"google.golang.org/protobuf/proto"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
)

// With is a shortcut.
func With(attrs ...*Attribute) *LogPrinter {
	return Default().Printer(1).With(attrs...)
}

// Info is a shortcut.
func Info() *LogPrinter {
	return Default().Printer(1).Info()
}

// Warning is a shortcut.
func Warning(err error) *LogPrinter {
	return Default().Printer(1).Warning(err)
}

// Error is a shortcut.
func Error(err error) *LogPrinter {
	return Default().Printer(1).Error(err)
}

// Critical is a shortcut.
func Critical(err error) *LogPrinter {
	return Default().Printer(1).Critical(err)
}

// Fatal is a shortcut.
func Fatal(err error) *LogPrinter {
	return Default().Printer(1).Fatal(err)
}

// Print is a shortcut.
func Print(message string) {
	Default().Printer(1).Print(message)
}

// Printf is a shortcut.
func Printf(format string, args ...interface{}) {
	Default().Printer(1).Printf(format, args...)
}

// Infof is a shortcut.
func Infof(format string, args ...interface{}) {
	Default().Printer(1).Infof(format, args...)
}

// Warningf is a shortcut.
func Warningf(format string, args ...interface{}) error {
	return Default().Printer(1).Warningf(format, args...)
}

// Errorf is a shortcut.
func Errorf(format string, args ...interface{}) error {
	return Default().Printer(1).Errorf(format, args...)
}

// Criticalf is a shortcut.
func Criticalf(format string, args ...interface{}) error {
	return Default().Printer(1).Criticalf(format, args...)
}

// Fatalf is a shortcut.
func Fatalf(format string, args ...interface{}) {
	Default().Printer(1).Fatalf(format, args...)
}

// PrintProto is a shortcut.
func PrintProto(prefix string, msg proto.Message) {
	Default().Printer(1).PrintProto(prefix, msg)
}

// PrintJSON is a shortcut.
func PrintJSON(prefix string, obj interface{}) {
	Default().Printer(1).PrintJSON(prefix, obj)
}

// EmitLogEntry is a shortcut.
func EmitLogEntry(entry *logspb.LogEntry) {
	Default().EmitLogEntry(entry)
}
