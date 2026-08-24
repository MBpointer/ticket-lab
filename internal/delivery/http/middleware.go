package http

import (
	"log/slog"
	"net/http"
	"time"
)

// LoggerMiddleware — структурированное логирование HTTP-запросов
func LoggerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Передаем управление следующему обработчику
		next.ServeHTTP(w, r)

		// Пишем один чистый JSON-лог со всеми метаданными запроса
		slog.Info("HTTP Request processed",
			"method", r.Method,
			"path", r.URL.Path,
			"remote_addr", r.RemoteAddr,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

// RecoveryMiddleware — спасение от паник с записью структурированной ошибки
func RecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				// Критические ошибки и паники пишем на уровне Error с контекстом
				slog.Error("🔥 PANIC RECOVERED IN HTTP SERVER",
					"error", err,
					"path", r.URL.Path,
					"method", r.Method,
				)

				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
