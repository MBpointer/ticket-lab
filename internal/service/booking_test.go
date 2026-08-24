package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"ticket-lab/internal/domain"
)

// ==========================================
// 1. ПОЛНЫЙ МОК ДЛЯ REDIS (Все методы интерфейса)
// ==========================================
type MockRedisRepo struct {
	ShouldFail bool
	IsReserved bool
}

// Метод 1: Резервирование места
func (m *MockRedisRepo) ReserveSeat(ctx context.Context, eventID, seatID, orderID string) (bool, error) {
	if m.ShouldFail {
		return false, errors.New("redis: connection refused")
	}
	return m.IsReserved, nil
}

// ДОБАВЛЯЕМ Метод 2: Освобождение места (Отмена брони по крону)
func (m *MockRedisRepo) ReleaseSeat(ctx context.Context, eventID, seatID, orderID string) (bool, error) {
	if m.ShouldFail {
		return false, errors.New("redis: failed to release seat")
	}
	return true, nil
}

// Метод 3: Распределенная блокировка
func (m *MockRedisRepo) AcquireTransactionLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if m.ShouldFail {
		return false, errors.New("redis: lock failed due to network error")
	}
	return true, nil
}

// Метод 4: Кеширование статуса одной транзакции
func (m *MockRedisRepo) CacheTransactionStatus(ctx context.Context, orderID string, status string, ttl time.Duration) error {
	if m.ShouldFail {
		return errors.New("redis: failed to cache status")
	}
	return nil
}

// Метод 5: Пакетное кеширование статусов транзакций
func (m *MockRedisRepo) CacheTransactionStatusesBatch(ctx context.Context, orderIDs []string, status string, ttl time.Duration) error {
	if m.ShouldFail {
		return errors.New("redis: failed to cache batch statuses")
	}
	return nil
}

// Метод 6: Получение статуса транзакции
func (m *MockRedisRepo) GetTransactionStatus(ctx context.Context, orderID string) (string, error) {
	if m.ShouldFail {
		return "", errors.New("redis: failed to get status")
	}
	if m.IsReserved {
		return "SUCCESS", nil
	}
	return "NOT_FOUND", nil
}

// ==========================================
// 2. МОК ДЛЯ KAFKA
// ==========================================
type MockKafkaProducer struct{}

func (m *MockKafkaProducer) PublishOrder(ctx context.Context, order *domain.Order) error {
	return nil
}

// ==========================================
// 3. ТЕСТ БИЗНЕС-ЛОГИКИ
// ==========================================
func TestBookingService_CreateBooking(t *testing.T) {
	tests := []struct {
		name         string
		mockReserved bool
		mockFail     bool
		expectError  bool
	}{
		{
			name:         "Успешное бронирование билета",
			mockReserved: true,
			mockFail:     false,
			expectError:  false,
		},
		{
			name:         "Ошибка: место уже забронировано (овербукинг)",
			mockReserved: false,
			mockFail:     false,
			expectError:  true,
		},
		{
			name:         "Ошибка: падение Redis по таймауту",
			mockReserved: false,
			mockFail:     true,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			mockRedis := &MockRedisRepo{
				ShouldFail: tt.mockFail,
				IsReserved: tt.mockReserved,
			}

			mockKafka := &MockKafkaProducer{}

			// Теперь абсолютно все методы интерфейса реализованы корректно
			service := NewBookingService(mockRedis, mockKafka)

			_, err := service.BookTicket(ctx, "event-123", "seat-45")

			if (err != nil) != tt.expectError {
				t.Errorf("Сценарий [%s]: Ожидали ошибку = %v, но получили err = %v", tt.name, tt.expectError, err)
			}
		})
	}
}
