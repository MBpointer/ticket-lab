package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Счетчик успешных бронирований через API
	BookedTicketsCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ticket_booked_total",
		Help: "The total number of successfully booked tickets.",
	})

	// Счетчик билетов, отмененных Кроном по таймауту
	CancelledTicketsCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "expired_tickets_cancelled_total",
		Help: "The total number of expired tickets cancelled by background cron worker.",
	})

	// Счетчик успешных возвратов мест в Redis после отмены брони
	CronReleasedTicketsCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ticket_cron_released_total",
		Help: "The total number of tickets successfully returned to circulation by cron.",
	})

	// Счетчик ошибок при возврате мест в Redis
	CronReleaseErrorsCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ticket_cron_release_errors_total",
		Help: "The total number of errors when cron tried to release seats in Redis.",
	})

	// Гистограмма времени выполнения операций Redis.
	// Label operation позволяет видеть p95 отдельно для каждой операции.
	RedisOperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ticket_redis_operation_duration_seconds",
			Help:    "Duration of Redis operations in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation"},
	)

	// Гистограмма времени пакетной записи заказов в PostgreSQL.
	PostgresBatchDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name: "postgres_batch_duration_seconds",
			Help: "Duration of PostgreSQL batch writes in seconds.",
			Buckets: []float64{
				0.001,
				0.005,
				0.01,
				0.05,
				0.1,
				0.5,
				1.0,
				5.0,
			},
		},
	)

	// Гистограмма времени выполнения операций Kafka.
	// Label operation позволяет разделять producer/consumer операции.
	KafkaOperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "ticket_kafka_operation_duration_seconds",
			Help:    "Duration of Kafka operations in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation"},
	)
)
