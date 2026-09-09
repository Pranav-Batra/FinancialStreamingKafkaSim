package events

type PaymentEvent struct {
    EventID     string `json:"event_id"`
    UserID      string `json:"user_id"`
    AmountCents int64  `json:"amount_cents"`
    Merchant    string `json:"merchant"`
    Status      string `json:"status"`
    EventTime   int64  `json:"event_time"`
}