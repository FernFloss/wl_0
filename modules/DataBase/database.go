package database

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"project_wb_l0/modules/consumer"
	"project_wb_l0/modules/general"
	"sync"

	_ "github.com/lib/pq"
)

type Db struct {
	db *sql.DB
}

// инициализируем базу данных и подключение к ней
func InitBd(ctx context.Context, user, password, name, host, port string) (*Db, error) {
	log.Println("Инициализируемся")
	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, name,
	)
	log.Println("Подключаемся")
	log.Println(connStr)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("ошибка открытия подключения: %w", err)
	}
	log.Println("Пингуемся")
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ошибка подключения к БД: %w", err)
	}
	log.Println("Запускаемся")
	database := &Db{db: db}
	return database, nil

}

// Устанавливаем связь между  чтение из кафки и бд
func (db *Db) StartListeningFromKafkaToWrite(ctx context.Context, fetchers ...consumer.Registration) {
	if db == nil || db.db == nil {
		log.Println("База данных не инициализирована")
		return
	}
	go db.listenFromKafkaToWrite(ctx, fetchers...)
}

// Слушаем и обрабатываем информацию с нескольких консюмеров
func (db *Db) listenFromKafkaToWrite(ctx context.Context, fetchers ...consumer.Registration) {
	var wg sync.WaitGroup
	wg.Add(len(fetchers))
	for _, f := range fetchers {
		fetcher := f
		go func() {
			defer wg.Done()
			for {
				select {
				case recievedData, ok := <-fetcher.Send():
					if !ok {
						log.Println("Канал fetcher.Send() закрыт")
						return
					}

					log.Println("Получили данные")
					log.Println(recievedData.Payment)
					err := db.writeOrder2Bd(recievedData)

					select {
					case fetcher.RecieveAnswer() <- consumer.Answer{Err: err}:
					default:
						log.Println("Не могу отправить ответ: канал занят или закрыт")
					}
				case <-ctx.Done():
					log.Println("горутина обрабатывающая фетчеры покидает это все дело ")
					return
				}
			}
		}()
	}
	go func() {
		wg.Wait()
		db.db.Close()
	}()
}

// Запись заказа в базу данных
func (d *Db) writeOrder2Bd(order general.Order) error {
	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("ошибка начала транзакции: %w", err)
	}
	// Откатываемся, если появилась ошибка
	defer tx.Rollback()

	// 1. Сохраняем Delivery
	_, err = tx.Exec(`
        INSERT INTO Delivery (name, phone, zip, city, address, region, email)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (name) DO UPDATE SET 
            phone = EXCLUDED.phone,
            zip = EXCLUDED.zip,
            city = EXCLUDED.city,
            address = EXCLUDED.address,
            region = EXCLUDED.region,
            email = EXCLUDED.email
        `,
		order.Delivery.Name,
		order.Delivery.Phone,
		order.Delivery.Zip,
		order.Delivery.City,
		order.Delivery.Address,
		order.Delivery.Region,
		order.Delivery.Email,
	)
	if err != nil {
		return fmt.Errorf("ошибка сохранения Delivery: %w", err)
	}

	// 2. Сохраняем Payment
	_, err = tx.Exec(`
        INSERT INTO Payment (
            transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
        ) ON CONFLICT (transaction) DO UPDATE SET
		 request_id = EXCLUDED.request_id,
        currency = EXCLUDED.currency,
        provider = EXCLUDED.provider,
        amount = EXCLUDED.amount,
        payment_dt = EXCLUDED.payment_dt,
        bank = EXCLUDED.bank,
        delivery_cost = EXCLUDED.delivery_cost,
        goods_total = EXCLUDED.goods_total,
        custom_fee = EXCLUDED.custom_fee
    `,
		order.Payment.Transaction,
		order.Payment.RequestID,
		order.Payment.Currency,
		order.Payment.Provider,
		order.Payment.Amount,
		order.Payment.PaymentDT,
		order.Payment.Bank,
		order.Payment.DeliveryCost,
		order.Payment.GoodsTotal,
		order.Payment.CustomFee,
	)
	if err != nil {
		return fmt.Errorf("ошибка сохранения Payment: %w", err)
	}

	// 3. Сохраняем Items
	for _, item := range order.Items {
		_, err = tx.Exec(`
            INSERT INTO Items (
                chrt_id, price, rid, name, sale, total_price, nm_id, brand, status
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9
            ) ON CONFLICT (chrt_id) DO UPDATE SET
			price = EXCLUDED.price,
			rid = EXCLUDED.rid,
			name = EXCLUDED.name,
			sale = EXCLUDED.sale,
			total_price = EXCLUDED.total_price,
			nm_id = EXCLUDED.nm_id,
			brand = EXCLUDED.brand,
			status = EXCLUDED.status 
			`,
			item.ChrtID,
			item.Price,
			item.Rid,
			item.Name,
			item.Sale,
			item.TotalPrice,
			item.NmID,
			item.Brand,
			item.Status,
		)
		if err != nil {
			return fmt.Errorf("ошибка сохранения Item: %w", err)
		}
	}

	// 4. Сохраняем сам Order
	_, err = tx.Exec(`
        INSERT INTO Orders (
            order_uid, entry, delivery_id, payment_id, track_number, locale, internal_signature,
            customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
        ) ON CONFLICT (order_uid) DO UPDATE SET
		entry = EXCLUDED.entry,
		delivery_id = EXCLUDED.delivery_id,
		payment_id = EXCLUDED.payment_id,
		track_number = EXCLUDED.track_number,
		locale = EXCLUDED.locale,
		internal_signature = EXCLUDED.internal_signature,
		customer_id = EXCLUDED.customer_id,
		delivery_service = EXCLUDED.delivery_service,
		shardkey = EXCLUDED.shardkey,
		sm_id = EXCLUDED.sm_id,
		date_created = EXCLUDED.date_created,
		oof_shard = EXCLUDED.oof_shard
	`,
		order.OrderUID,
		order.Entry,
		order.Delivery.Name,
		order.Payment.Transaction,
		order.TrackNumber,
		order.Locale,
		order.InternalSignature,
		order.CustomerID,
		order.DeliveryService,
		order.Shardkey,
		order.SmID,
		order.DateCreated,
		order.OofShard,
	)
	if err != nil {
		return fmt.Errorf("ошибка сохранения Order: %w", err)
	}

	// 5. Связываем Order и Items через Order_contents
	for _, item := range order.Items {
		_, err = tx.Exec(`
            INSERT INTO Order_contents (order_uid, chrt_id)
            VALUES ($1, $2)
            ON CONFLICT (order_uid, chrt_id) DO UPDATE SET
			order_uid = EXCLUDED.order_uid,
			chrt_id = EXCLUDED.chrt_id`,
			order.OrderUID,
			item.ChrtID,
		)
		if err != nil {
			return fmt.Errorf("ошибка сохранения Order_contents: %w", err)
		}
	}

	// Завершаем транзакцию
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("ошибка завершения транзакции: %w", err)
	}

	log.Println("Заказ успешно записан в БД")
	return nil
}

// Получаем информацию о заказе с UID заказа
func (d *Db) GetOrderByUID(uid string, order *general.Order) error {
	// Получаем основной заказ
	err := d.db.QueryRow(`
        SELECT 
            order_uid, entry, delivery_id, payment_id, track_number, locale, internal_signature,
            customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard
        FROM Orders
        WHERE order_uid = $1`, uid).Scan(
		&order.OrderUID,
		&order.Entry,
		&order.Delivery.Name,
		&order.Payment.Transaction,
		&order.TrackNumber,
		&order.Locale,
		&order.InternalSignature,
		&order.CustomerID,
		&order.DeliveryService,
		&order.Shardkey,
		&order.SmID,
		&order.DateCreated,
		&order.OofShard,
	)
	if err != nil {
		return fmt.Errorf("не найдено в Orders: %w", err)
	}

	// Получаем Delivery
	err = d.db.QueryRow(`SELECT
							name, phone, zip, city, address,
						 	region, email	
							FROM Delivery	
							WHERE name = $1`, order.Delivery.Name).Scan(
		&order.Delivery.Name,
		&order.Delivery.Phone,
		&order.Delivery.Zip,
		&order.Delivery.City,
		&order.Delivery.Address,
		&order.Delivery.Region,
		&order.Delivery.Email,
	)
	if err != nil {
		return fmt.Errorf("ошибка загрузки Delivery: %w", err)
	}

	// Получаем Payment
	err = d.db.QueryRow(`SELECT
							transaction, request_id, currency, provider, amount,
							payment_dt, bank, delivery_cost, goods_total, custom_fee
						FROM Payment
						WHERE transaction = $1`, order.Payment.Transaction).Scan(
		&order.Payment.Transaction,
		&order.Payment.RequestID,
		&order.Payment.Currency,
		&order.Payment.Provider,
		&order.Payment.Amount,
		&order.Payment.PaymentDT,
		&order.Payment.Bank,
		&order.Payment.DeliveryCost,
		&order.Payment.GoodsTotal,
		&order.Payment.CustomFee,
	)
	if err != nil {
		return fmt.Errorf("ошибка загрузки Payment: %w", err)
	}

	// Получаем Items
	rows, err := d.db.Query(`SELECT
							oc.chrt_id, price, rid, name, sale, total_price,
							nm_id, brand, status
							FROM Items i JOIN Order_contents oc ON i.chrt_id = oc.chrt_id WHERE oc.order_uid = $1`, uid)
	if err != nil {
		return fmt.Errorf("ошибка загрузки Items: %w", err)
	}
	defer rows.Close()

	var items []general.Item
	for rows.Next() {
		var item general.Item
		if err := rows.Scan(
			&item.ChrtID,
			&item.Price,
			&item.Rid,
			&item.Name,
			&item.Sale,
			&item.TotalPrice,
			&item.NmID,
			&item.Brand,
			&item.Status,
		); err != nil {
			return fmt.Errorf("ошибка сканирования Item: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("ошибка при чтении Items: %w", err)
	}

	order.Items = items
	return nil
}

// Сохраняем UID в HASH
func (d *Db) SaveOrderToCacheBd(uid string) error {

	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("ошибка начала транзакции: %w", err)
	}

	// Откатываемся, если появилась ошибка
	defer tx.Rollback()
	log.Println("Сохраняем в HASH ", uid)

	// 6. Добавляем Hash
	_, err = tx.Exec(`
        INSERT INTO Hash (order_id)
        VALUES ($1)
        ON CONFLICT (order_id) DO NOTHING`,
		uid,
	)
	if err != nil {
		return fmt.Errorf("ошибка сохранения Hash: %w", err)
	}
	// Завершаем транзакцию
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("ошибка завершения транзакции: %w", err)
	}
	return nil

}

// RemoveFromHash удаляет запись из таблицы Hash по UID
func (d *Db) RemoveFromHash(uid string) error {
	result, err := d.db.Exec("DELETE FROM Hash WHERE order_id = $1", uid)
	if err != nil {
		return fmt.Errorf("ошибка удаления из Hash: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("запись с UID %s не найдена в Hash", uid)
	}

	log.Printf("Запись %s удалена из Hash", uid)
	return nil
}

// LoadCacheFromDB — загружает кэш из таблицы Hash
func (d *Db) GetAllHashUIDs() ([]string, error) {
	rows, err := d.db.Query("SELECT order_id FROM Hash")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var uids []string
	for rows.Next() {
		var uid string
		if err := rows.Scan(&uid); err != nil {
			return nil, fmt.Errorf("ошибка сканирования Hash: %w", err)
		}
		uids = append(uids, uid)
	}

	return uids, nil
}
