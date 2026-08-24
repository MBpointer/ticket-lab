package logger

import (
	"log/slog"
	"os"
)

func Init(service string) {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})

	logger := slog.New(handler).With(
		slog.String("service_name", service),
		slog.String("env", "development"),
	)

	slog.SetDefault(logger)

	slog.Info("logger initialized")
}
