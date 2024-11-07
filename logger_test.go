package cc

import (
	"context"
	"log/slog"
	"testing"

	"github.com/google/uuid"
)

func TestLogger(t *testing.T) {
	ctx := context.Background()
	//compute := uuid.New()
	//event := uuid.New()
	manifest := uuid.New()
	payload := uuid.New()
	logger := NewCcLogger(CcLoggerInput{manifest.String(), payload.String(), nil})
	logger.Log(ctx, LevelAction, "My Message")
	logger.Info("TEST Info")
	logger.Debug("TEST Debug")
	logger.Error("Test Error")
	logger.Warn("Test Warn")
	logger.Action("TEST Action")
	logger.SendMessage("KANAWHA", "TestMessage", slog.Attr{"arg1", slog.StringValue("val1")})
}
