package producer

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaProducer struct {
	writer *kafka.Writer
}

func MustInit(ctx context.Context) *KafkaProducer {
	kafkaAddr := os.Getenv("KAFKA_ADDR")
	if kafkaAddr == "" {
		kafkaAddr = "localhost:9092"
	}
	brokers := []string{kafkaAddr}
	topic := "orders"

	var conn *kafka.Conn
	var err error

	// Настраиваем 5 попыток с паузой в 1 секунду
	maxRetries := 5
	for i := 1; i <= maxRetries; i++ {
		slog.Info(
			"🔄 [Kafka Producer] Connecting to broker...",
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
				// Успех, Кафка полностью готова принимать сообщения от API
				break
			}
		}

		slog.Warn(
			"⚠️ [Kafka Producer] Broker is not ready yet, waiting...",
			"error", err,
			"address", kafkaAddr,
		)

		time.Sleep(1 * time.Second)
	}

	// Если после всех попыток подключиться не удалось — жестко выходим
	if err != nil {
		slog.Error(
			"❌ CRITICAL KAFKA PRODUCER ERROR: Connection failed after all retries.",
			"error", err,
			"address", kafkaAddr,
		)
		os.Exit(1)
	}

	// Закрываем проверочное соединение перед созданием постоянного Writer
	if closeErr := conn.Close(); closeErr != nil {
		slog.Warn("⚠️ [Kafka Producer] Failed to close health-check connection", "error", closeErr)
	}

	slog.Info("🦅 [Kafka Producer] Connectivity verified successfully (Ping OK).", "address", kafkaAddr)

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		Async:        true,
		BatchSize:    1000,
		BatchTimeout: 10 * time.Millisecond,
	}

	return &KafkaProducer{writer: w}
}

func (p *KafkaProducer) Close() {
	if p.writer != nil {
		slog.Info("🔌 [Kafka Producer] Closing writer...")
		if err := p.writer.Close(); err != nil {
			slog.Error("❌ [Kafka Producer] Error while closing writer", "error", err)
		}
	}
}
