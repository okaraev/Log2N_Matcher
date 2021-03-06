package main

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"time"

	"github.com/streadway/amqp"
)

type Breaker struct {
	Status           string        // Breaker Status
	FailCount        int           // Failed operations' count
	LastFail         time.Time     // Succeeded operations' count
	FailThreshold    int           // Failed operations threshold
	SuccessThreshold time.Duration // Time duration in which all operations must be succeeded after that FailCount will reset and Status will change to 'Closed'
	OpenThreshold    time.Duration // Time duration after which Status will change to 'HalfOpen'
	Operation        func(q, qq string, i interface{}) error
}

func ReceiveMessage(RMQServer string, queue string) (<-chan amqp.Delivery, error) {
	connectRabbitMQ, err := amqp.Dial(RMQServer)
	if err != nil {
		return nil, err
	}
	channelRabbitMQ, err := connectRabbitMQ.Channel()
	if err != nil {
		connectRabbitMQ.Close()
		return nil, err
	}
	consumerName := os.Getenv("COMPUTERNAME")
	messages, err := channelRabbitMQ.Consume(queue, consumerName, false, false, false, false, nil)
	if err != nil {
		connectRabbitMQ.Close()
		channelRabbitMQ.Close()
		return nil, err
	}
	return (<-chan amqp.Delivery)(messages), nil
}

func SendMessage(amqpServerURL string, QName string, i interface{}) error {
	connectRabbitMQ, err := amqp.Dial(amqpServerURL)
	if err != nil {
		return err
	}
	defer connectRabbitMQ.Close()
	channelRabbitMQ, err := connectRabbitMQ.Channel()
	if err != nil {
		return err
	}
	defer channelRabbitMQ.Close()
	bytes, err := json.Marshal(i)
	if err != nil {
		return err
	}
	message := amqp.Publishing{
		ContentType: "application/json",
		Body:        bytes,
	}
	err = channelRabbitMQ.Publish("", QName, false, false, message)
	if err != nil {
		return err
	}
	return nil
}

func (b *Breaker) New(myFunc func(q, qq string, i interface{}) error) {
	b.Status = "Closed"
	b.OpenThreshold = 30 * time.Second
	b.FailCount = 0
	b.FailThreshold = 3
	b.LastFail = time.Now()
	b.SuccessThreshold = 1 * time.Minute
	b.Operation = myFunc
}

func (b *Breaker) Open() {
	go func() {
		b.Status = "Open"
		time.Sleep(b.OpenThreshold)
		b.Status = "HalfOpen"
	}()
}

func (b *Breaker) Do(AMQPServer string, QName string, i interface{}) error {
	// IF Connection is OK and Fail threshold is exceeded mark connection as fail for a time for a fast fail
	if b.Status == "Closed" && b.FailCount >= b.FailThreshold {
		b.Open()
		return errors.New("fail treshold exceeded")
	}
	// IF connection marked as fail, return immediate error
	if b.Status == "Open" {
		return errors.New("fail treshold exceeded")
	}
	// DO operation and check result
	err := b.Operation(AMQPServer, QName, i)
	if err != nil {
		if b.Status == "HalfOpen" {
			b.Open()
		} else {
			b.FailCount++
		}
		b.LastFail = time.Now()
		return err
	}
	// IF connection is marked as Healthy or halfHealthy check for Last Failed time
	if b.Status == "HalfOpen" || b.Status == "Closed" {
		if time.Since(b.LastFail) >= b.SuccessThreshold {
			b.Close()
		}
	}
	return nil
}

func (b *Breaker) Close() {
	b.Status = "Closed"
	b.FailCount = 0
}

type Retrier struct {
	Status      string        // Retrier Status
	HoldTime    time.Duration // Time duration after which Operation will retry. If Operation fails hold time multiplies until 360 seconds
	MaxHoldTime time.Duration
	Operation   func(q string, qq string) (<-chan amqp.Delivery, error)
}

func (r *Retrier) New(myFunc func(q string, qq string) (<-chan amqp.Delivery, error)) {
	r.HoldTime = 10 * time.Second
	r.MaxHoldTime = 360 * time.Second
	r.Status = "Closed"
	r.Operation = myFunc
}

func (r *Retrier) Open() {
	r.Status = "Open"
}

func (r *Retrier) Close() {
	r.HoldTime = 10 * time.Second
	r.Status = "Closed"
}

func (r *Retrier) Multiply() {
	if r.HoldTime <= r.MaxHoldTime {
		r.HoldTime = r.HoldTime * 2
	}
}

func (r *Retrier) isClosed() bool {
	return r.Status == "Closed"
}

func (r *Retrier) Do(RMQServer string, QName string) <-chan amqp.Delivery {
	result, err := r.Operation(RMQServer, QName)
	mydelivery := make(chan amqp.Delivery)
	if err != nil {
		r.Open()
		log.Println(err)
	}
	if r.isClosed() {
		// if status is success starting routine to receive messages
		go func() {
			for message := range result {
				mydelivery <- message
			}
			// Ending routine when error happened and setting status to fail
			r.Open()
		}()
	}
	// Starting forever loop in routine to check if status is fail
	go func() {
		for {
			time.Sleep(1 * time.Second)
			if !r.isClosed() {
				// if status is fail waiting for defined hold time and trying again
				time.Sleep(r.HoldTime)
				retryResult, err := r.Operation(RMQServer, QName)
				if err != nil {
					r.Multiply()
					log.Println(err)
				} else {
					// if retry is success setting status to success and starting routine to receive messages
					r.Close()
					go func() {
						for message := range retryResult {
							mydelivery <- message
						}
						// Ending routine when error happened and setting status to fail
						r.Open()
					}()
				}
			}
		}
	}()
	return (<-chan amqp.Delivery)(mydelivery)
}
