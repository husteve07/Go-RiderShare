package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"ride-sharing/shared/contracts"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	TripExchange = "trip"
)

type RabbitMQ struct {
	conn    *amqp.Connection
	Channel *amqp.Channel
}

func NewRabbitMQ(uri string) (*RabbitMQ, error) {
	conn, err := amqp.Dial(uri)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open a channel: %v", err)
	}

	rmq := &RabbitMQ{
		conn:    conn,
		Channel: ch,
	}

	if err := rmq.setupExchangesAndQueues(); err != nil {
		rmq.Close()
		return nil, fmt.Errorf("failed to setup exchanges and queues: %v", err)
	}

	return rmq, nil
}

type MessageHandler func(context.Context, amqp.Delivery) error

func (r *RabbitMQ) ConsumeMessages(queueName string, handler MessageHandler) error {

	err := r.Channel.Qos(
		1,
		0,
		false,
	)
	if err != nil {
		return fmt.Errorf("failed to set QoS: %v", err)
	}

	msgs, err := r.Channel.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return fmt.Errorf("failed to register a consumer: %v", err)
	}

	ctx := context.Background()
	go func() {
		for msg := range msgs {
			log.Printf("Received a message: %s", msg.Body)
			if err := handler(ctx, msg); err != nil {
				log.Printf("ERROR: failed to handle message: %v", err)

				if nackErr := msg.Nack(false, false); nackErr != nil {
					log.Printf("ERROR: failed to nack message: %v", nackErr)
				}
				continue
			}
			if ackErr := msg.Ack(false); ackErr != nil {
				log.Printf("ERROR: failed to ack message: %v", ackErr)
			}
		}

	}()

	return nil
}

func (r *RabbitMQ) PublishMessage(ctx context.Context, routingKey string, message contracts.AmqpMessage) error {
	log.Printf("Publishing message to routing key %s: %s", routingKey, message)

	jsonMsg, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	return r.Channel.PublishWithContext(
		ctx,
		TripExchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         jsonMsg,
			DeliveryMode: amqp.Persistent,
		})
}

func (r *RabbitMQ) setupExchangesAndQueues() error {
	err := r.Channel.ExchangeDeclare(
		TripExchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange: %v", err)
	}

	if err := r.declareAndBindQueue(
		FindAvailableDriversQueue,
		[]string{
			contracts.TripEventCreated, contracts.TripEventDriverNotInterested,
		},
		TripExchange,
	); err != nil {
		return err
	}

	if err := r.declareAndBindQueue(
		DriverCmdTripRequestQueue,
		[]string{contracts.DriverCmdTripRequest},
		TripExchange,
	); err != nil {
		return err
	}

	if err := r.declareAndBindQueue(
		DriverTripResponseQueue,
		[]string{contracts.DriverCmdTripAccept, contracts.DriverCmdTripDecline},
		TripExchange,
	); err != nil {
		return err
	}

	if err := r.declareAndBindQueue(
		NotifyDriverNoDriversFoundQueue,
		[]string{contracts.TripEventNoDriversFound},
		TripExchange,
	); err != nil {
		return err
	}

	if err := r.declareAndBindQueue(
		NotifyDriverAssignQueue,
		[]string{contracts.TripEventDriverAssigned},
		TripExchange,
	); err != nil {
		return err
	}

	if err := r.declareAndBindQueue(
		PaymentTripResponseQueue,
		[]string{contracts.PaymentCmdCreateSession},
		TripExchange,
	); err != nil {
		return err
	}

	if err := r.declareAndBindQueue(
		NotifyPaymentSessionCreatedQueue,
		[]string{contracts.PaymentEventSessionCreated},
		TripExchange,
	); err != nil {
		return err
	}

	if err := r.declareAndBindQueue(
		NotifyPaymentSuccessQueue,
		[]string{contracts.PaymentEventSuccess},
		TripExchange,
	); err != nil {
		return err
	}

	return nil
}

func (r *RabbitMQ) declareAndBindQueue(queueName string, messageTypes []string, exchange string) error {
	q, err := r.Channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %v", err)
	}

	for _, messageType := range messageTypes {
		if err := r.Channel.QueueBind(
			q.Name,
			messageType,
			exchange,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("failed to bind queue: %v", err)
		}
	}

	return nil
}

func (r *RabbitMQ) Close() {
	if r.conn != nil {
		r.conn.Close()
	}
	if r.Channel != nil {
		r.Channel.Close()
	}
}
