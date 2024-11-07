package cc

import (
	"context"
	"log/slog"
	"os"
)

const (
	LevelAction      = slog.Level(41)
	LevelSendMessage = slog.Level(42)
)

var LevelNames = map[slog.Leveler]string{
	LevelAction:      "ACTION",
	LevelSendMessage: "SENDMESSAGE",
}

type MessageWriter interface {
	Write(p []byte) (n int, err error)
	Close()
}

type CcLoggerInput struct {
	Manifest      string
	Payload       string
	MessageWriter MessageWriter
}

type CcLogger struct {
	input CcLoggerInput
	*slog.Logger
	messageLogger *slog.Logger
}

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

func (l *CcLogger) Action(msg string, args ...slog.Attr) {
	ctx := context.Background()
	l.Log(ctx, LevelAction, msg)
}

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

func toAny(attr []slog.Attr) []any {
	size := len(attr)
	a := make([]any, size)
	for i := 0; i < size; i++ {
		a[i] = attr[i]
	}
	return a
}

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
