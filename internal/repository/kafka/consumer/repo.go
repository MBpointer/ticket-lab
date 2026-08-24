package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"ticket-lab/internal/metrics"
)

// FetchRawMessage забирает сырое сообщение из Kafka.
//
// Полное Kafka Message сохраняется внутри consumer,
// чтобы потом корректно выполнить CommitMessages().
func (c *KafkaConsumer) FetchRawMessage(
	ctx context.Context,
) (string, []byte, int64, error) {

	start := time.Now()

	defer func() {
		metrics.KafkaOperationDuration.
			WithLabelValues("fetch_message").
			Observe(time.Since(start).Seconds())
	}()

	slog.Info("Kafka FetchMessage waiting")
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		slog.Warn("Kafka FetchMessage failed", "error", err)
		return "", nil, 0, fmt.Errorf(
			"failed to fetch message from kafka: %w",
			err,
		)
	}
	slog.Info(
		"Kafka message fetched",
		"offset", msg.Offset,
		"partition", msg.Partition,
	)
	if err != nil {
		return "", nil, 0, fmt.Errorf(
			"failed to fetch message from kafka: %w",
			err,
		)
	}

	// Сохраняем полное сообщение для последующего commit.
	c.lastMessage = msg

	return string(msg.Key), msg.Value, msg.Offset, nil
}

// CommitOffset подтверждает обработку последнего
// успешно записанного в PostgreSQL сообщения.
func (c *KafkaConsumer) CommitOffset(
	ctx context.Context,
	offset int64,
) error {

	start := time.Now()

	defer func() {
		metrics.KafkaOperationDuration.
			WithLabelValues("commit_offset").
			Observe(time.Since(start).Seconds())
	}()

	if c.lastMessage.Topic == "" {
		return fmt.Errorf(
			"cannot commit kafka offset %d: last fetched message is empty",
			offset,
		)
	}

	if c.lastMessage.Offset != offset {
		return fmt.Errorf(
			"cannot commit kafka offset %d: last fetched message has offset %d",
			offset,
			c.lastMessage.Offset,
		)
	}

	err := c.reader.CommitMessages(
		ctx,
		c.lastMessage,
	)

	if err != nil {
		return fmt.Errorf(
			"failed to commit kafka offset %d: %w",
			offset,
			err,
		)
	}

	return nil
}
