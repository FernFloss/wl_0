package cache

import (
	"fmt"
	"log"
	"math/rand"
	database "project_wb_l0/modules/DataBase"
	"project_wb_l0/modules/general"
	"sync"
)

// Cache представляет собой простой in-memory кэш с рандомным удалением
type Cache struct {
	maxItems int
	data     map[string]general.Order
	db       *database.Db
	mu       sync.RWMutex
}

// NewCache создаёт новый кэш с заданным максимальным размером
func NewCache(maxItems int, db *database.Db) *Cache {
	return &Cache{
		maxItems: maxItems,
		data:     make(map[string]general.Order),
		db:       db,
	}
}

// // Get — получает заказ из кэша или БД
func (c *Cache) Get(uid string) (general.Order, error) {
	c.mu.RLock()
	order, ok := c.data[uid]
	c.mu.RUnlock()
	log.Println("Ищем в кэше")
	if ok {
		return order, nil
	}
	log.Println("Не нашли в кэш, ищем в бд")
	// Если нет в кэше — загружаем из БД
	var dbOrder general.Order
	err := c.db.GetOrderByUID(uid, &dbOrder)
	if err != nil {
		var empty general.Order
		return empty, err
	}
	log.Println("Сохраняем в кэш")
	// Сохраняем в кэш
	c.Set(uid, dbOrder)
	return dbOrder, nil
}

// Set — добавляет заказ в кэш и в таблицу  Hash из бд
func (c *Cache) Set(uid string, order general.Order) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.data) >= c.maxItems {
		c.mu.Unlock()
		err := c.evict()
		c.mu.Lock()
		if err != nil {
			return
		}
	}
	c.data[uid] = order
	err := c.db.SaveOrderToCacheBd(uid)
	if err != nil {
		log.Println("Ошибка сохранения ", uid, "в HASH: ", err)
	}
}

// evict удаляет случайную запись из кэша и из таблицы Hash в БД
func (c *Cache) evict() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.data) == 0 {
		return nil
	}
	keys := make([]string, 0, len(c.data))
	for k := range c.data {
		keys = append(keys, k)
	}
	// Выбираем случайный ключ
	idx := rand.Intn(len(keys))
	uidToDelete := keys[idx]

	// Удаляем из кэша
	delete(c.data, uidToDelete)

	// Удаляем из Hash через БД
	err := c.db.RemoveFromHash(uidToDelete)
	if err != nil {
		log.Printf("Не удалось удалить запись из Hash: %v", err)
		return err
	}
	return nil
}

// RestoreFromDB восстанавливает кэш из БД:
// 1. Забирает все order_id из таблицы Hash
// 2. Для каждого UID загружает заказ через db.GetOrderByUID()
// 3. Сохраняет в локальный кэш
func (c *Cache) RestoreFromDB() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Шаг 1: Получаем список UID'ов из Hash
	uids, err := c.db.GetAllHashUIDs()
	if err != nil {
		return fmt.Errorf("ошибка получения UID'ов из Hash: %w", err)
	}

	log.Printf("Начинаем восстановление кэша из БД. Найдено записей: %d\n", len(uids))

	// Шаг 2: Для каждого UID загружаем данные из БД и кладём в кэш
	for _, uid := range uids {
		var order general.Order
		if err := c.db.GetOrderByUID(uid, &order); err != nil {
			log.Printf("Не удалось восстановить заказ %s: %v", uid, err)
			continue
		}

		c.data[uid] = order
		log.Printf("Заказ %s успешно восстановлен в кэше", uid)
	}

	return nil
}
