package cc

import (
	"context"
	"fmt"
	"log/slog"
	"os"
)

const (
	LevelAction      = slog.Level(41)
	LevelSendMessage = slog.Level(42)
	LevelFatal       = slog.Level(99)
)

// LevelNames maps custom log levels to their string representations.
var LevelNames = map[slog.Leveler]string{
	LevelAction:      "ACTION",
	LevelSendMessage: "SENDMESSAGE",
	LevelFatal:       "FATAL",
}

// MessageWriter is an interface for writing message logs.
type MessageWriter interface {
	Write(p []byte) (n int, err error)
	Close()
}

// CcLoggerInput holds the input parameters required to create a CcLogger instance.
type CcLoggerInput struct {
	Manifest      string
	Payload       string
	MessageWriter MessageWriter
}

// cc logger provides logging capabilities with custom log levels and handlers.
type CcLogger struct {
	input CcLoggerInput
	*slog.Logger
	messageLogger *slog.Logger
}

// NewCcLogger creates a new instance of CcLogger with the provided input parameters.
func NewCcLogger(input CcLoggerInput) *CcLogger {
	stdOutLogger := slog.New(slog.NewJSONHandler(os.Stdout, ccLoggerOpts(slog.LevelDebug)))
	var messageLogger *slog.Logger
	if input.MessageWriter != nil {
		messageLogger = slog.New(slog.NewJSONHandler(input.MessageWriter, ccLoggerOpts(LevelSendMessage)))
	}
	return &CcLogger{
		input,
		stdOutLogger,
		messageLogger,
	}
}

// Action logs an action-related message with the specified log level and attributes.
// func (l *CcLogger) Action(msg string, args ...slog.Attr) {
// 	ctx := context.Background()
// 	l.Logger.Log(ctx, LevelAction, msg, args...)
// }

// Action logs an action-related message with the specified log level and attributes.
func (l *CcLogger) Action(msg string, args ...any) {
	ctx := context.Background()
	l.Logger.Log(ctx, LevelAction, msg, args...)
}

func (l *CcLogger) Actionf(msg string, args ...any) {
	ctx := context.Background()
	l.Logger.Log(ctx, LevelAction, fmt.Sprintf(msg, args...))
}

// SendMessage logs a send message event to channel
func (l *CcLogger) SendMessage(channel string, msg string, args ...slog.Attr) {
	ctx := context.Background()
	attrs := toAny(args)
	attrs = append(attrs, slog.Attr{
		Key:   "channel",
		Value: slog.StringValue(channel),
	})
	attrs = append(attrs, slog.Attr{
		Key:   "manifest",
		Value: slog.StringValue(l.input.Manifest),
	})
	attrs = append(attrs, slog.Attr{
		Key:   "payload",
		Value: slog.StringValue(l.input.Payload),
	})
	l.Log(ctx, LevelSendMessage, msg, attrs...)

	//send to message handler if it exists
	if l.messageLogger != nil {
		l.messageLogger.Log(ctx, LevelSendMessage, msg, attrs...)
	}
}

// Fatal logs a fatal error message and exits the application.
func (l *CcLogger) Fatal(msg string, args ...slog.Attr) {
	ctx := context.Background()
	l.Log(ctx, LevelAction, msg)
	os.Exit(1)
}

// Fatalf logs a formatted fatal error message and exits the application.
func (l *CcLogger) Fatalf(msg ...string) {
	ctx := context.Background()
	errmessage := msg[0]
	l.Log(ctx, LevelAction, fmt.Sprintf(errmessage, sliceToAnySlice(msg[1:])...))
	os.Exit(1)
}

// toAny converts a slice of slog.Attr to a slice of any.
func toAny(attr []slog.Attr) []any {
	size := len(attr)
	a := make([]any, size)
	for i := 0; i < size; i++ {
		a[i] = attr[i]
	}
	return a
}

// fromAny converts a slice of any to a slice of slog.Attr.
func fromAny(slice []any) []slog.Attr {
	attrs := make([]slog.Attr, len(slice))
	for i, v := range slice {
		key := fmt.Sprintf("item-%d", i)
		attrs[i] = slog.Any(key, v)
	}
	return attrs
}

// sliceToAnySlice converts a slice of any type to a slice of any.
func sliceToAnySlice[T any](t []T) []any {
	anyslice := make([]any, len(t))
	for i, v := range t {
		anyslice[i] = v
	}
	return anyslice
}

// ccLoggerOpts returns slog.HandlerOptions configured with custom log levels and attribute replacement.
func ccLoggerOpts(cclevel slog.Level) *slog.HandlerOptions {
	return &slog.HandlerOptions{
		Level: cclevel,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.LevelKey {
				level := a.Value.Any().(slog.Level)
				levelLabel, exists := LevelNames[level]
				if !exists {
					levelLabel = level.String()
				}
				a.Value = slog.StringValue(levelLabel)
			}
			return a
		},
	}
}
