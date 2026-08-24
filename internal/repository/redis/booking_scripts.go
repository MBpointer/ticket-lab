package redis

import "github.com/redis/go-redis/v9"

const (
	// Lua-скрипт атомарно резервирует существующее свободное место.
	//
	// ВАЖНО:
	// отсутствие поля означает, что такого места НЕ существует.
	// Поэтому мы больше не используем HGET == false как признак
	// свободного места.
	reserveSeatScript = `
		local current = redis.call("HGET", KEYS[1], ARGV[1])

		if current == ARGV[3] then
			redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
			return 1
		end

		return 0
	`

	// Lua-скрипт освобождает место только если текущий владелец
	// совпадает с orderID.
	//
	// После освобождения место НЕ удаляется из Hash.
	// Оно возвращается в состояние AVAILABLE.
	releaseSeatScript = `
		local current_owner = redis.call("HGET", KEYS[1], ARGV[1])

		if current_owner == ARGV[2] then
			redis.call("HSET", KEYS[1], ARGV[1], ARGV[3])
			return 1
		end

		return 0
	`
)

var (
	reserveSeat          = redis.NewScript(reserveSeatScript)
	releaseSeatScriptObj = redis.NewScript(releaseSeatScript)
)
