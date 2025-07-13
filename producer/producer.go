package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

type Order struct {
	OrderUID          string    `json:"order_uid"`
	TrackNumber       string    `json:"track_number"`
	Entry             string    `json:"entry"`
	Delivery          Delivery  `json:"delivery"`
	Payment           Payment   `json:"payment"`
	Items             []Item    `json:"items"`
	Locale            string    `json:"locale"`
	InternalSignature string    `json:"internal_signature"`
	CustomerID        string    `json:"customer_id"`
	DeliveryService   string    `json:"delivery_service"`
	Shardkey          string    `json:"shardkey"`
	SmID              string    `json:"sm_id"`
	DateCreated       time.Time `json:"date_created"`
	OofShard          string    `json:"oof_shard"`
}

type Delivery struct {
	Name    string `json:"name"`
	Phone   string `json:"phone"`
	Zip     string `json:"zip"`
	City    string `json:"city"`
	Address string `json:"address"`
	Region  string `json:"region"`
	Email   string `json:"email"`
}

type Payment struct {
	Transaction  string    `json:"transaction"`
	RequestID    string    `json:"request_id"`
	Currency     string    `json:"currency"`
	Provider     string    `json:"provider"`
	Amount       float64   `json:"amount"`
	PaymentDT    time.Time `json:"payment_dt"`
	Bank         string    `json:"bank"`
	DeliveryCost float64   `json:"delivery_cost"`
	GoodsTotal   float64   `json:"goods_total"`
	CustomFee    float64   `json:"custom_fee"`
}

type Item struct {
	ChrtID      string  `json:"chrt_id"`
	TrackNumber string  `json:"track_number"`
	Price       float64 `json:"price"`
	Rid         string  `json:"rid"`
	Name        string  `json:"name"`
	Sale        int     `json:"sale"`
	Size        string  `json:"size"`
	TotalPrice  float64 `json:"total_price"`
	NmID        string  `json:"nm_id"`
	Brand       string  `json:"brand"`
	Status      int     `json:"status"`
}

func randomChoice(word []string) string {
	possibleChoice := append(word, "")
	return possibleChoice[rand.Intn(len(possibleChoice))]

}

func generateRandomOrder() Order {
	// что встречается чаще одного раза в полях
	orderUID := randomChoice([]string{randomString(16)})             //randomChoice([]string{randomString(16)})
	trackNumber := randomChoice([]string{"WBILM" + randomString(5)}) //randomChoice([]string{"WBILM" + randomString(5)})
	name := randomChoice([]string{"Wildberries Logistics",
		"AliExpress Express",
		"Yandex Market",
		"Ozon Delivery",
		"Meest Express",
	})

	return Order{
		OrderUID:          orderUID,
		TrackNumber:       trackNumber,
		Entry:             "WBIL",
		Locale:            "en",
		InternalSignature: "",
		CustomerID:        "test",
		DeliveryService:   "meest",
		Shardkey:          "9",
		SmID:              "99",
		DateCreated:       time.Now().UTC(),
		OofShard:          "1",

		Delivery: Delivery{
			Name:    name,
			Phone:   "+9720000000",
			Zip:     "2639809",
			City:    "Kiryat Mozkin",
			Address: "Ploshad Mira 15",
			Region:  "Kraiot",
			Email:   "test@gmail.com",
		},

		Payment: Payment{
			Transaction:  randomChoice([]string{orderUID}), //randomChoice([]string{orderUID})
			RequestID:    "",
			Currency:     "USD",
			Provider:     "wbpay",
			Amount:       1817,
			PaymentDT:    time.Now().UTC(),
			Bank:         "alpha",
			DeliveryCost: 1500,
			GoodsTotal:   317,
			CustomFee:    float64(rand.Intn(100)),
		},

		Items: []Item{
			{
				ChrtID:      strconv.Itoa(rand.Intn(9999999-1000000+1) + 1000000),
				TrackNumber: randomChoice([]string{trackNumber}), //randomChoice([]string{trackNumber})
				Price:       453,
				Rid:         randomString(15),
				Name:        "Mascaras",
				Sale:        30,
				Size:        "0",
				TotalPrice:  317,
				NmID:        strconv.Itoa(2389212),
				Brand:       "Vivienne Sabo",
				Status:      202,
			},
		},
	}
}

func randomString(n int) string {
	letter := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	b := make([]rune, n)
	for i := range b {
		b[i] = letter[rand.Intn(len(letter))]
	}
	return string(b)
}

func main() {

	ctx := context.Background()

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "order-info",
	})
	defer writer.Close()

	log.Println("Запускаем producer... Отправляем сообщения в Kafka")

	for {
		order := generateRandomOrder()
		body, err := json.Marshal(order)

		if err != nil {
			log.Printf("Ошибка при json: %v\n", err)
		}

		err = writer.WriteMessages(ctx,
			kafka.Message{
				Key:   []byte(order.OrderUID),
				Value: body,
			},
		)

		if err != nil {
			log.Printf("Ошибка при отправке: %v\n", err)
		} else {
			log.Printf("Отправлено сообщение: \n")
		}

		// Ждём от 60 секунд до 2.5 минут
		delay := time.Duration(10+rand.Intn(20)) * time.Second
		time.Sleep(delay)
	}
}
