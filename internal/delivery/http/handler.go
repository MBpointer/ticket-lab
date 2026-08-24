package http

import (
	"encoding/json"
	"net/http"

	"ticket-lab/internal/service"
)

type BookingHandler struct {
	bookingService *service.BookingService
}

func NewBookingHandler(s *service.BookingService) *BookingHandler {
	return &BookingHandler{bookingService: s}
}

type requestBody struct {
	TicketID string `json:"ticket_id"`
	UserID   string `json:"user_id"`
}

// BookTicketHandler обрабатывает входящие запросы на покупку билета
func (h *BookingHandler) BookTicketHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			respondWithError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		// Защита от DDoS (максимум 4КБ) и гарантированное закрытие body
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		defer func() { _ = r.Body.Close() }()

		var body requestBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.TicketID == "" || body.UserID == "" {
			respondWithError(w, http.StatusBadRequest, "invalid request body or limit exceeded")
			return
		}

		// Вызов слоя бизнес-логики (Use Cases / Core)
		order, err := h.bookingService.BookTicket(r.Context(), body.TicketID, body.UserID)
		if err != nil {
			mapAndRespondError(w, err, body.TicketID, body.UserID)
			return
		}

		respondWithJSON(w, http.StatusCreated, order)
	}
}
