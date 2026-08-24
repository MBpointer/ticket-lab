package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

const (
	apiURL = "http://localhost:8080/book"

	// На концерте ровно 10 000 мест:
	// ticket_0 ... ticket_9999.
	totalSeats = 10000
)

func main() {
	fmt.Println("🚀 Истинный эмулятор пользователей запущен!")
	fmt.Printf(
		"🎯 Отправляем боевые POST-запросы на %s\n",
		apiURL,
	)
	fmt.Printf(
		"🎟️ Инвентарь концерта: %d мест (ticket_0 ... ticket_%d)\n",
		totalSeats,
		totalSeats-1,
	)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	client := &http.Client{
		Timeout: 1 * time.Second,
	}

	for {
		// Имитируем высокую нагрузку:
		// запрос каждые 5–50 миллисекунд.
		//sleepTime := time.Duration(
		//	r.Intn(45)+5,
		//) * time.Millisecond

		//time.Sleep(sleepTime)

		// Генерируем ТОЛЬКО существующие места.
		ticketID := fmt.Sprintf(
			"ticket_%d",
			r.Intn(totalSeats),
		)

		userID := fmt.Sprintf(
			"user_%d",
			r.Intn(99999),
		)

		jsonBody := fmt.Sprintf(
			`{"ticket_id":"%s","user_id":"%s"}`,
			ticketID,
			userID,
		)

		reqBody := strings.NewReader(jsonBody)

		resp, err := client.Post(
			apiURL,
			"application/json",
			reqBody,
		)

		if err != nil {
			fmt.Printf(
				"❌ Ошибка отправки запроса на сервер: %v\n",
				err,
			)
			continue
		}

		fmt.Printf(
			"🎯 %s → %s\n",
			ticketID,
			resp.Status,
		)

		resp.Body.Close()
	}
}
