package domain

// Константы для фиксации состояний жизненного цикла заказа
const (
	StatusReserved  = "RESERVED"
	StatusPaid      = "PAID"
	StatusCancelled = "CANCELLED"
)

// Order — главная Highload-модель заказа
type Order struct {
	ID        string `json:"id"`
	TicketID  string `json:"ticket_id"`
	UserID    string `json:"user_id"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
}

// Ticket — структура метаданных билета, необходимая для работы кэша Redis
type Ticket struct {
	ID      string `json:"id"`
	EventID string `json:"event_id"`
	Price   int64  `json:"price"`
}

// ExpiredOrder представляет информацию о просроченном заказе
// для освобождения места в Redis.
type ExpiredOrder struct {
	ID       string `json:"id"`
	TicketID string `json:"ticket_id"`
}
