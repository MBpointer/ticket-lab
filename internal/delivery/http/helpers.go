package http

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"ticket-lab/internal/service"
)

// mapAndRespondError изолирует логику подбора HTTP-статусов под бизнес-ошибки
func mapAndRespondError(w http.ResponseWriter, err error, ticketID, userID string) {
	if errors.Is(err, service.ErrNoTickets) {
		respondWithError(w, http.StatusConflict, "no tickets left")
		return
	}
	if errors.Is(err, service.ErrDuplicateBooking) {
		respondWithError(w, http.StatusTooManyRequests, "duplicate request, please wait")
		return
	}

	// Критическую неизвестную ошибку логируем со сквозным контекстом
	slog.Error("Internal error during ticket booking",
		"error", err,
		"ticket_id", ticketID,
		"user_id", userID,
	)
	respondWithError(w, http.StatusInternalServerError, "internal server error")
}

// respondWithError стандартизирует формат ошибок приложения без лишних аллокаций
func respondWithError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(`{"error":"` + message + `"}`))
}

// respondWithJSON стандартизирует успешные ответы
func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		slog.Error("Failed to encode response JSON", "error", err)
	}
}
