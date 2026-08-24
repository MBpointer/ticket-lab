-- 1. Таблица заказов с оптимизированными под Highload типами данных
CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY,                 -- Нативный 16-байтный UUID (в 2 раза компактнее VARCHAR)
    ticket_id VARCHAR(50) NOT NULL,      -- ID конкретного места/сиденья (seat_id)
    user_id VARCHAR(50) NOT NULL,        -- ID покупателя
    status VARCHAR(20) NOT NULL,         -- RESERVED, PAID, CANCELLED
    created_at TIMESTAMPTZ NOT NULL     -- Временная метка с таймзоной для Крона
);

-- 2. 🛡️ ЗАЩИТА ОТ ОВЕРБУКИНГА (Частичный уникальный индекс):
-- Гарантирует, что на одно место (ticket_id) может существовать только ОДИН активный 
-- заказ в статусе RESERVED или PAID. Если Крон отменил заказ (статус стал CANCELLED), 
-- место автоматически вылетает из этого индекса, позволяя купить его заново.
-- База жестко отвергнет любой дубликат из Kafka.
CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_active_ticket 
ON orders (ticket_id) 
WHERE status IN ('RESERVED', 'PAID');

-- 3. 🔥 ОПТИМИЗАЦИЯ ДЛЯ КРОНА (Частичный B-Tree индекс):
-- Индекс хранит данные ТОЛЬКО по забронированным билетам, которые ожидают оплаты.
-- Исторические оплаченные (PAID) и отмененные (CANCELLED) заказы сюда не попадают. 
-- Крон находит просроченные брони мгновенно, не нагружая диск и RAM.
CREATE INDEX IF NOT EXISTS idx_orders_expired_holds 
ON orders (created_at) 
WHERE status = 'RESERVED';

-- Таблица инвентаря мест (Source of Truth)
CREATE TABLE IF NOT EXISTS seats (
    id SERIAL PRIMARY KEY,
    seat_id VARCHAR(50) NOT NULL UNIQUE,
    status VARCHAR(20) NOT NULL DEFAULT 'AVAILABLE' -- AVAILABLE или BOOKED
);

-- Быстрая автогенерация 10 000 мест при самом первом запуске базы
INSERT INTO seats (seat_id, status)
SELECT 'ticket_' || g, 'AVAILABLE'
FROM generate_series(0, 9999) AS g
ON CONFLICT (seat_id) DO NOTHING;

-- Индекс для мгновенной выборки свободных мест при прогреве кэша
CREATE INDEX IF NOT EXISTS idx_seats_available ON seats (seat_id) WHERE status = 'AVAILABLE';
