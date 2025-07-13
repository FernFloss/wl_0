package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"project_wb_l0/modules/general"

	"github.com/segmentio/kafka-go"
)

type Answer struct {
	Err error
}

type Consumer struct {
	reader     *kafka.Reader
	answerBd   chan Answer
	sendDataBd chan general.Order
}

// Канал для чтения информации о заказе
func (c *Consumer) Send() <-chan general.Order {
	return c.sendDataBd
}

// Канал для принятия ответом
func (c *Consumer) RecieveAnswer() chan<- Answer {
	return c.answerBd
}

type Registration interface {
	Send() <-chan general.Order
	RecieveAnswer() chan<- Answer
}

// инициализируем консюмер и зупаскаем чтение из кафки и отправки дальше по каналу sendDataBd через метод Send
func InitConsumer(ctx context.Context, brokers []string, topic, groupID string, checkFrequency int) Registration {
	c := &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			Topic:          topic,
			GroupID:        groupID,
			CommitInterval: 0, // Отключаем автоматический коммит
		}),
		sendDataBd: make(chan general.Order),
		answerBd:   make(chan Answer),
	}
	go c.serve(ctx, checkFrequency)
	return c
}

// функция для чтение данных из кафки
// валидации ей по структуру Order, если данные не подходят под валидацию - алерт и коммит ( чтобы читать дальше)
// отправки на бд
// после получения ответа от бд - коммитив
func (c *Consumer) fetch(ctx context.Context) {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		log.Printf("Проблема с чтение данных из кафки: %v\n", err)
		return
	}
	validatedData := validateOrder(msg.Value)
	if validatedData.Err != nil {
		//отправляем алерт, что что-то не так
		log.Printf("Проблемы с валидацией данных: %v\n в topic=%s, partition=%d, offset=%d \n", validatedData.Err,
			msg.Topic,
			msg.Partition,
			msg.Offset)
		// Коммитим оффсет вручную после обработки
		err = c.reader.CommitMessages(ctx, msg)
		if err != nil {
			panic(err)
		}
		return
	}

	for {
		select {
		case c.sendDataBd <- validatedData.Order:
			//логика после отправки данных в бд
			for {
				select {
				case answer := <-c.answerBd:
					if answer.Err != nil {
						log.Printf("Проблемы с записью заказа в базу данных: %v\n", answer.Err)
					}

					// Коммитим оффсет вручную после обработки
					err = c.reader.CommitMessages(context.Background(), msg)
					if err != nil {
						panic(err)
					}
					//типа конец, отправили на бд  и что-то получили теперь можно и выходить из попыток
					log.Printf("Данные успешно записаны в бд из topic=%s, partition=%d, offset=%d \n",
						msg.Topic,
						msg.Partition,
						msg.Offset)
					return
				case <-ctx.Done():
					return
				}
			}
		case <-ctx.Done():
			//надо возвращаться
			return
		}
	}

}

func (c *Consumer) serve(ctx context.Context, checkFrequency int) {
	clock := time.NewTicker(time.Duration(checkFrequency) * time.Second)
	for {
		select {
		case <-clock.C:
			c.fetch(ctx)
		case <-ctx.Done():
			close(c.sendDataBd)
			close(c.answerBd)
			return
		}
	}
}

func ValidateTrackNumbers(order general.Order) error {
	if order.TrackNumber == "" {
		return errors.New("track_number в заказе не задан")
	}

	for i, item := range order.Items {
		if item.TrackNumber != order.TrackNumber {
			return fmt.Errorf("item[%d] имеет неверный track_number: %s вместо %s",
				i, item.TrackNumber, order.TrackNumber)
		}
	}

	return nil
}

func validateOrder(result []byte) general.ValidateResult {
	order := general.Order{}
	err := json.Unmarshal(result, &order)
	if err != nil {
		return general.ValidateResult{Order: order, Err: err}
	}
	if order.OrderUID == "" {
		return general.ValidateResult{Order: order, Err: errors.New("поле ID не заполнено")}
	}
	if order.Delivery.Name == "" {
		return general.ValidateResult{Order: order, Err: errors.New("поле name  отнощения Delivery не заполнено")}
	}
	if order.Payment.Transaction == "" {
		return general.ValidateResult{Order: order, Err: errors.New("поле transaction отношения Payment не заполнено")}
	}
	err = ValidateTrackNumbers(order)
	if err != nil {
		return general.ValidateResult{Order: order, Err: err}
	}
	return general.ValidateResult{Order: order, Err: nil}

}
