package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	database "project_wb_l0/modules/DataBase"
	cache "project_wb_l0/modules/cache"
	"project_wb_l0/modules/config"
	"project_wb_l0/modules/consumer"
	"syscall"

	"github.com/gin-gonic/gin"
)

// getOrderByID — обработчик Gin для получения заказа по ID.
// cache.Get(id) - сначала ищет в кэше и только при необходимости обращается к БД.
func getOrderByID(c *gin.Context, cache *cache.Cache) {
	id := c.Param("id")
	log.Printf("Ищем заказ с UID: %s", id)

	order, err := cache.Get(id)
	if err != nil {
		log.Println(err)
	}

	c.JSON(http.StatusOK, order)
}

// RegisterWebRoutes — регистрирует маршруты веб-интерфейса.
func RegisterWebRoutes(r *gin.Engine) {
	r.LoadHTMLGlob("templates/*.html")

	r.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.html", nil)
	})
}

func main() {

	// Насртойка для graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Обработка сигналов ОС (Ctrl+C и т.п.)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		fmt.Printf("\nПoлeчtн сигнал %v: завершение работы...\n", sig)
		cancel()
	}()

	// Подключение к базе данных
	db, err := database.InitBd(ctx,
		config.DBUser,
		config.DBPassword,
		config.DBName,
		config.DBHost,
		config.DBPort,
	)
	if err != nil {
		log.Println("Ошибка при подключении к бд")
		log.Println(err)
	}

	// Инициализируем кэш
	cache := cache.NewCache(config.CacheMaxItems, db)
	log.Println("Кэш настроен")

	// Восстановление кэша их БД
	if err := cache.RestoreFromDB(); err != nil {
		log.Printf("Предупреждение: не удалось восстановить кэш из БД: %v", err)
	} else {
		log.Println("Кэш успешно восстановлен из БД")
	}

	// Запуск консьюмера\ов для кафки и подключение их к бд
	c1 := consumer.InitConsumer(ctx,
		[]string{config.KafkaBroker},
		config.KafkaTopic,
		config.KafkaGroupID,
		int(config.KafkaFetchWait.Seconds()),
	)
	log.Println(int(config.KafkaFetchWait.Seconds()))
	db.StartListeningFromKafkaToWrite(ctx, c1)

	// Настройка Gin HTTP сервера
	router := gin.Default()
	RegisterWebRoutes(router)

	router.GET("/order/:id", func(c *gin.Context) {
		getOrderByID(c, cache)
	})

	srv := &http.Server{
		Addr:    ":5000",
		Handler: router,
	}

	// Запускаем сервер в горутине
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Ошибка запуска HTTP сервера: %v\n", err)
		}
	}()
	log.Println("HTTP сервер запущен на :5000")

	// Завершение работы
	<-ctx.Done()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Ошибка при завершении сервера: %v\n", err)
	}
	log.Println("HTTP сервер остановлен")

	fmt.Println("Работа завершена. Выходим из main()")
}
