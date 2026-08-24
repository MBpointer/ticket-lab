package consumer

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaConsumer struct {
	reader *kafka.Reader

	// Последнее реально прочитанное сообщение.
	// Оно необходимо для корректного CommitMessages().
	lastMessage kafka.Message
}

// MustInit создает KafkaConsumer и проводит жесткий Health Check брокера.
func MustInit(ctx context.Context) *KafkaConsumer {
	kafkaAddr := os.Getenv("KAFKA_ADDR")

	if kafkaAddr == "" {
		kafkaAddr = "127.0.0.1:9092"
	}

	var conn *kafka.Conn
	var err error

	// Настраиваем 5 попыток с паузой в 1 секунду
	maxRetries := 5
	for i := 1; i <= maxRetries; i++ {
		slog.Info(
			"🔄 [Kafka Consumer] Connecting to broker...",
			"attempt", i,
			"max", maxRetries,
			"address", kafkaAddr,
		)

		// Выделяем на каждую попытку короткий таймаут в 3 секунды
		healthCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		conn, err = kafka.DialContext(healthCtx, "tcp", kafkaAddr)
		cancel()

		if err == nil {
			// Если физически подключились, проверяем контроллер метаданных
			_, err = conn.Controller()
			if err == nil {
				// Успех, Кафка полностью готова принимать трафик
				break
			}
		}

		slog.Warn(
			"⚠️ [Kafka Consumer] Broker is not ready yet, waiting...",
			"error", err,
			"address", kafkaAddr,
		)

		time.Sleep(1 * time.Second)
	}

	// Если после всех попыток подключиться не удалось — жестко выходим
	if err != nil {
		slog.Error(
			"❌ CRITICAL KAFKA CONSUMER ERROR: Connection failed after all retries.",
			"error", err,
			"address", kafkaAddr,
		)
		os.Exit(1)
	}

	// Закрываем проверочное соединение перед созданием постоянного Reader
	if closeErr := conn.Close(); closeErr != nil {
		slog.Warn(
			"⚠️ [Kafka Consumer] Failed to close health-check connection",
			"error", closeErr,
		)
	}

	slog.Info(
		"📥 [Kafka Consumer] Connectivity verified successfully",
		"address", kafkaAddr,
	)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaAddr},
		Topic:   "orders",
		GroupID: "ticket-batch-workers",

		// Забираем сообщения небольшими порциями.
		MinBytes: 1,
		MaxBytes: 10e6,

		// Для НОВОЙ consumer group начинаем с начала topic.
		StartOffset: kafka.FirstOffset,

		// Явно задаем commit interval.
		CommitInterval: 0,
	})

	slog.Info(
		"📥 [Kafka Consumer] Reader initialized",
		"topic", "orders",
		"group", "ticket-batch-workers",
		"address", kafkaAddr,
	)

	return &KafkaConsumer{
		reader: reader,
	}
}

// Close безопасно закрывает внутренний reader.
func (c *KafkaConsumer) Close() {
	if c == nil || c.reader == nil {
		return
	}

	slog.Info("🔌 [Kafka Consumer] Closing reader...")

	if err := c.reader.Close(); err != nil {
		slog.Error(
			"❌ [Kafka Consumer] Error while closing reader",
			"error", err,
		)
	}
}
