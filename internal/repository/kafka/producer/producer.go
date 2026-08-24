package producer

import (
	"context"
	"encoding/json"
	"fmt"

	"ticket-lab/internal/domain"

	"github.com/segmentio/kafka-go"
)

// PublishOrder отправляет сериализованный заказ в топик Kafka
func (p *KafkaProducer) PublishOrder(ctx context.Context, order *domain.Order) error {
	payload, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("failed to marshal order: %w", err)
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(order.ID),
		Value: payload,
	})
	if err != nil {
		return fmt.Errorf("failed to publish order to kafka: %w", err)
	}

	return nil
}
