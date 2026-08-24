package http

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"ticket-lab/internal/domain"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HTTPServer инкапсулирует в себе всю логику работы с сетью HTTP,
// скрывая детали реализации (таймауты, роутинг) от main.go.
type HTTPServer struct {
	server    *http.Server
	redisRepo domain.RedisRepository
}

// NewHTTPServer — конструктор, который собирает роутинг, эндпоинты и Middleware.
func NewHTTPServer(addr string, handler *BookingHandler, redisRepo domain.RedisRepository) *HTTPServer {
	mux := http.NewServeMux()

	// Регистрируем эндпоинты
	mux.HandleFunc("/book", handler.BookTicketHandler())
	mux.Handle("/metrics", promhttp.Handler())

	// Навешиваем Middleware цепочкой
	finalHandler := LoggerMiddleware(RecoveryMiddleware(mux))

	srv := &http.Server{
		Addr:         addr,
		Handler:      finalHandler,
		ReadTimeout:  5 * time.Second, // Защита от Highload-зависаний сокетов
		WriteTimeout: 10 * time.Second,
	}

	return &HTTPServer{
		server:    srv,
		redisRepo: redisRepo,
	}
}

// Start запускает сервер в отдельной горутине, чтобы не блокировать основной поток.
func (s *HTTPServer) Start(ctx context.Context) {
	go func() {
		slog.Info("🚀 HTTP API Server is starting...", "addr", s.server.Addr)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("❌ Critical: HTTP server failed to listen and serve", "error", err)
		}
	}()
}

// WaitForShutdown инкапсулирует логику мягкой остановки (Graceful Shutdown).
// Выделяет серверу 5 секунд на завершение текущих запросов клиентов.
func (s *HTTPServer) WaitForShutdown() {
	slog.Info("🛑 Stopping API Server gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := s.server.Shutdown(shutdownCtx); err != nil {
		slog.Error("❌ Error during HTTP API Server graceful shutdown", "error", err)
		return
	}
	slog.Info("👋 API Server process successfully and cleanly finished.")
}
