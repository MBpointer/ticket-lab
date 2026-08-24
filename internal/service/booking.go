package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"ticket-lab/internal/domain"
	"ticket-lab/internal/metrics"

	"github.com/google/uuid"
)

var ErrNoTickets = errors.New("no tickets available")
var ErrDuplicateBooking = errors.New("you have already requested a booking for this ticket")

// defaultEventID — храним eventID в одном месте, чтобы не хардкодить в сервисе.
const defaultEventID = "rock_concert_2026"

type BookingService struct {
	redisRepo domain.RedisRepository
	queueRepo domain.KafkaProducerRepository
}

func NewBookingService(r domain.RedisRepository, q domain.KafkaProducerRepository) *BookingService {
	return &BookingService{redisRepo: r, queueRepo: q}
}

// BookTicket обрабатывает запрос на бронирование конкретного места.
func (s *BookingService) BookTicket(ctx context.Context, ticketID, userID string) (*domain.Order, error) {
	eventID := defaultEventID

	// 1. LOCK: блокируем по eventID+ticketID, чтобы конкурентные запросы
	// на одно и то же место реально блокировали друг друга.
	lockKey := fmt.Sprintf("%s:%s", eventID, ticketID)
	acquired, err := s.redisRepo.AcquireTransactionLock(ctx, lockKey, 15*time.Second)
	if err != nil {
		return nil, fmt.Errorf("idempotency check failed: %w", err)
	}
	if !acquired {
		return nil, ErrDuplicateBooking
	}

	// 2. ReserveSeat — атомарно занимаем место. orderID = уникальный UUID для каждого бронирования.
	orderID := uuid.New().String()
	success, err := s.redisRepo.ReserveSeat(ctx, eventID, ticketID, orderID)
	if err != nil {
		return nil, fmt.Errorf("failed to reserve seat in cache: %w", err)
	}
	if !success {
		return nil, ErrNoTickets
	}

	// 3. Создаём заказ
	order := &domain.Order{
		ID:        orderID,
		TicketID:  ticketID,
		UserID:    userID,
		Status:    domain.StatusReserved,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	// 4. Кэшируем PENDING статус
	_ = s.redisRepo.CacheTransactionStatus(ctx, order.ID, "PENDING", 5*time.Minute)

	// 5. Публикуем в Kafka
	err = s.queueRepo.PublishOrder(ctx, order)
	if err != nil {
		slog.Error("❌ Kafka publish failed. Compensating reservation in Redis",
			"error", err,
			"event_id", eventID,
			"seat_id", ticketID,
			"user_id", userID,
		)
		// Компенсируем: освобождаем место
		released, compErr := s.redisRepo.ReleaseSeat(ctx, eventID, ticketID, orderID)
		if compErr != nil {
			slog.Error("❌ Failed to compensate reservation on Kafka error",
				"error", compErr,
				"order_id", order.ID,
			)
		} else if !released {
			slog.Warn("⚠️ Seat was already released by another process during compensation",
				"order_id", order.ID,
			)
		}
		_ = s.redisRepo.CacheTransactionStatus(ctx, order.ID, "FAILED", 1*time.Minute)
		return nil, fmt.Errorf("failed to send order to queue: %w", err)
	}

	metrics.BookedTicketsCounter.Inc()
	return order, nil
}
