package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"
	"github.com/streadway/amqp"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error cargando archivo .env: %v", err)
	}

	rabbitURL := os.Getenv("RABBITMQ_URL")
	queueName := os.Getenv("FIRST_QUEUE")
	apiURL := os.Getenv("API_URL")

	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("Error conectando a RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Error abriendo canal RabbitMQ: %v", err)
	}
	defer ch.Close()

	msgs, err := ch.Consume(
		queueName, // Nombre de la cola
		"",        // Consumidor
		true,      // Auto-ack
		false,     // Exclusivo
		false,     // No-local
		false,     // No-wait
		nil,       // Args
	)
	if err != nil {
		log.Fatalf("Error al consumir mensajes: %v", err)
	}

	fmt.Println("Escuchando mensajes en la cola...")

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			fmt.Printf("Mensaje recibido: %s\n", d.Body)

			data := map[string]string{"msg": string(d.Body)}
			payload, err := json.Marshal(data)
			if err != nil {
				log.Printf("Error serializando JSON: %v", err)
				continue
			}
			fmt.Println("Enviando solicitud a:", apiURL)

			resp, err := http.Post(apiURL, "application/json", bytes.NewBuffer(payload))
			if err != nil {
				log.Printf("Error enviando POST a la API: %v", err)
				continue
			}
			defer resp.Body.Close()

			fmt.Printf("Mensaje enviado a la API, respuesta: %d\n", resp.StatusCode)
		}
	}()

	<-forever
}
