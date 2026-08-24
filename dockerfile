# === Шаг 1: Сборка бинарников в тяжелом образе Go ===
FROM golang:1.26-alpine AS builder
WORKDIR /app

# Копируем файлы зависимостей и скачиваем их
COPY go.mod go.sum ./
RUN go mod download

# Копируем весь исходный код проекта
COPY . .

# Собираем два независимых бинарника (исправлены пути к cmd/api)
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/api-server ./cmd/api/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/batch-worker ./cmd/worker/main.go

# === Шаг 2: Финальный ультра-легкий контейнер для запуска ===
FROM alpine:3.20
WORKDIR /root/

# Добавляем корневые сертификаты (нужны для безопасных сетевых запросов, если они появятся в коде)
RUN apk --no-cache add ca-certificates

# Копируем готовые бинарники из предыдущего шага сборщика
COPY --from=builder /app/api-server .
COPY --from=builder /app/batch-worker .

# По умолчанию контейнер ничего не запускает. 
# Конкретный исполняемый файл мы будем гибко задавать через команду 'command' в docker-compose.
