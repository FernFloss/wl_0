package config

import (
	"log"
	"os"
	"strconv"
	"time"
)

// Конфигурация БД
var (
	DBUser     = getEnv("DB_USER", "db_user")
	DBPassword = getEnv("DB_PASSWORD", "db_pass")
	DBName     = getEnv("DB_NAME", "order_db")
	DBHost     = getEnv("DB_HOST", "localhost")
	DBPort     = getEnv("DB_PORT", "5432")
)

// Конфигурация Kafka
var (
	KafkaBroker    = getEnv("KAFKA_BROKER", "localhost:9092")
	KafkaTopic     = getEnv("KAFKA_TOPIC", "order-info")
	KafkaGroupID   = getEnv("KAFKA_GROUP_ID", "OrderToBd")
	KafkaFetchWait = time.Second * time.Duration(getEnvAsInt("KAFKA_FETCH_WAIT", 5))
)

// Конфигурация HTTP-сервера
var (
	ServerAddr = getEnv("SERVER_ADDR", ":5000")
)

// Конфигурация кэша
var (
	CacheMaxItems = getEnvAsInt("CACHE_MAX_ITEMS", 20)
)

// getEnv возвращает значение из переменной окружения или значение по умолчанию
func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// getEnvAsInt читаем переменную как int
func getEnvAsInt(name string, defaultValue int) int {
	valStr := getEnv(name, "")
	if valStr == "" {
		return defaultValue
	}
	val, err := strconv.Atoi(valStr)
	if err != nil {
		log.Printf("Ошибка при парсинге %s: %v. Используется значение по умолчанию: %d", name, err, defaultValue)
		return defaultValue
	}
	return val
}
