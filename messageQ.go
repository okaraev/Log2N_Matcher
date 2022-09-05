package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/streadway/amqp"
)

func ReceiveMessage(ConnectionParams interface{}) (<-chan interface{}, error) {
	abstractchan := make(chan interface{})
	confParams, ok := ConnectionParams.(qconfig)
	if !ok {
		return nil, fmt.Errorf("connectionparams argument is not qconfig")
	}
	connectRabbitMQ, err := amqp.Dial(confParams.QConnectionString)
	if err != nil {
		return nil, err
	}
	channelRabbitMQ, err := connectRabbitMQ.Channel()
	if err != nil {
		connectRabbitMQ.Close()
		return nil, err
	}
	consumerName := os.Getenv("COMPUTERNAME")
	messages, err := channelRabbitMQ.Consume(confParams.QName, consumerName, false, false, false, false, nil)
	if err != nil {
		connectRabbitMQ.Close()
		channelRabbitMQ.Close()
		return nil, err
	}
	go func() {
		for mess := range messages {
			abstractchan <- mess
		}
	}()
	return abstractchan, nil
}

func ProcessMessage(message interface{}, CircuitBreaker Breaker, FM FileManager) error {
	delivery, ok := message.(amqp.Delivery)
	if !ok {
		return fmt.Errorf("message argument is not delivery")
	}
	mylog := Log{}
	err := json.Unmarshal(delivery.Body, &mylog)
	if err != nil {
		return err
	}
	configs, err := getConfig(mylog.Team)
	if err != nil {
		return err
	}
	notifications := mylog.MatchConfig(configs)
	for _, notification := range notifications {
		err = CircuitBreaker.Do(notification)
		if err != nil {
			err = FM.SendMessage(notification)
			if err != nil {
				return err
			}
		}
	}
	err = delivery.Ack(true)
	if err != nil {
		return err
	}
	return nil
}

func SendMessage(message interface{}, ConnectionParams interface{}) error {
	confParams, ok := ConnectionParams.(qconfig)
	if !ok {
		return fmt.Errorf("connectionparams argument is not qconfig")
	}
	connectRabbitMQ, err := amqp.Dial(confParams.QConnectionString)
	if err != nil {
		return err
	}
	defer connectRabbitMQ.Close()
	channelRabbitMQ, err := connectRabbitMQ.Channel()
	if err != nil {
		return err
	}
	defer channelRabbitMQ.Close()
	bytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	mess := amqp.Publishing{
		ContentType: "application/json",
		Body:        bytes,
	}
	err = channelRabbitMQ.Publish("", confParams.QName, false, false, mess)
	if err != nil {
		return err
	}
	return nil
}
