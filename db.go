package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/streadway/amqp"
)

func getConfig(team string) ([]TeamConfig, error) {
	configs := []TeamConfig{}
	rdb := redis.NewClient(&redis.Options{
		Addr: GlobalConfig.Cache,
	})
	ctx := context.Background()
	pattern := fmt.Sprintf("%s-*", team)
	keys, err := rdb.Keys(ctx, pattern).Result()
	if err != nil {
		return configs, err
	}
	defer rdb.Close()
	for _, key := range keys {
		value, err := rdb.Get(ctx, key).Bytes()
		if err != nil {
			return configs, err
		}
		config := TeamConfig{}
		err = json.Unmarshal(value, &config)
		if err != nil {
			return configs, err
		}
		configs = append(configs, config)
	}
	return configs, nil
}

func setConfig(teamConfig TeamConfig) error {
	rdb := redis.NewClient(&redis.Options{
		Addr: GlobalConfig.Cache,
	})
	ctx := context.Background()
	configbytes, err := json.Marshal(teamConfig)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s-%s", teamConfig.Team, teamConfig.Name)
	err = rdb.Set(ctx, key, configbytes, 0).Err()
	if err != nil {
		return err
	}
	defer rdb.Close()
	return nil
}

func removeConfig(teamConfig TeamConfig) error {
	rdb := redis.NewClient(&redis.Options{
		Addr: GlobalConfig.Cache,
	})
	ctx := context.Background()
	key := fmt.Sprintf("%s-%s", teamConfig.Team, teamConfig.Name)
	err := rdb.Del(ctx, key).Err()
	if err != nil {
		return err
	}
	defer rdb.Close()
	return nil
}

func processConfigUpdate(message interface{}, FM FileManager) error {
	delivery, ok := message.(amqp.Delivery)
	if !ok {
		return fmt.Errorf("message argument is not delivery")
	}
	myUpd := ConfigUPD{}
	err := json.Unmarshal(delivery.Body, &myUpd)
	if err != nil {
		return err
	}
	if myUpd.UpdateType != "Delete" {
		err := FM.SetConfig(myUpd.TeamConfig)
		if err != nil {
			return err
		}
	} else {
		err := FM.RemoveConfig(myUpd.TeamConfig)
		if err != nil {
			return err
		}
	}
	err = delivery.Ack(true)
	if err != nil {
		return err
	}
	return nil
}
