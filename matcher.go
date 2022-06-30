package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type qconfig struct {
	QConnectionString string
	QName             string
}

type webconfig struct {
	Cache       string
	QueueConfig []qconfig
}

var GlobalConfig webconfig

func throw(err error) {
	if err != nil {
		panic(err)
	}
}

func getEnvVars() error {
	rdb := os.Getenv("dbcs")
	PQName := os.Getenv("pqname")
	PQConStr := os.Getenv("pqconnectionstringPath")
	SQName := os.Getenv("sqname")
	SQConStr := os.Getenv("SQConnectionstringPath")
	LogQName := os.Getenv("logqname")
	LogQConStr := os.Getenv("logqconnectionstringPath")
	StatusQName := os.Getenv("statusqname")
	StatusQConStr := os.Getenv("statusqconnectionstringPath")
	if rdb == "" {
		return fmt.Errorf("cannot get dbcs environment variable")
	}
	bytes, err := os.ReadFile(rdb)
	if err != nil {
		return err
	}
	rdbcs := strings.Split(string(bytes), "\n")[0]
	if PQConStr == "" {
		return fmt.Errorf("cannot get pqconnectionstringpath environment variable")
	}
	bytes, err = os.ReadFile(PQConStr)
	if err != nil {
		return err
	}
	pqcs := strings.Split(string(bytes), "\n")[0]
	if PQName == "" {
		return fmt.Errorf("cannot get pqname environment variable")
	}
	if SQConStr == "" {
		return fmt.Errorf("cannot get sqconnectionstringpath environment variable")
	}
	bytes, err = os.ReadFile(SQConStr)
	if err != nil {
		return err
	}
	sqcs := strings.Split(string(bytes), "\n")[0]
	if SQName == "" {
		return fmt.Errorf("cannot get sqname environment variable")
	}
	if LogQName == "" {
		return fmt.Errorf("cannot get logqname environment variable")
	}
	if LogQConStr == "" {
		return fmt.Errorf("cannot get logqconnectionstringPath environment variable")
	}
	bytes, err = os.ReadFile(LogQConStr)
	if err != nil {
		return err
	}
	logqcs := strings.Split(string(bytes), "\n")[0]
	if StatusQName == "" {
		return fmt.Errorf("cannot get statusqname environment variable")
	}
	if StatusQConStr == "" {
		return fmt.Errorf("cannot get statusqconnectionstringPath environment variable")
	}
	bytes, err = os.ReadFile(StatusQConStr)
	if err != nil {
		return err
	}
	statusqcs := strings.Split(string(bytes), "\n")[0]
	q1 := qconfig{QConnectionString: logqcs, QName: LogQName}
	q2 := qconfig{QConnectionString: pqcs, QName: PQName}
	q3 := qconfig{QConnectionString: statusqcs, QName: StatusQName}
	q4 := qconfig{QConnectionString: sqcs, QName: SQName}
	qconf := []qconfig{q1, q2, q3, q4}
	wconf := webconfig{QueueConfig: qconf, Cache: rdbcs}
	GlobalConfig = wconf
	return nil
}

func main() {
	err := getEnvVars()
	throw(err)
	messages, err := ReceiveMessage(GlobalConfig.QueueConfig[0].QConnectionString, GlobalConfig.QueueConfig[0].QName)
	throw(err)
	updates, err := ReceiveMessage(GlobalConfig.QueueConfig[2].QConnectionString, GlobalConfig.QueueConfig[2].QName)
	throw(err)
	forever := make(chan bool)
	go func() {
		myBreaker := Breaker{}
		myBreaker.New(SendMessage)
		for message := range messages {
			mylog := Log{}
			err = json.Unmarshal(message.Body, &mylog)
			throw(err)
			configs, err := getConfig(mylog.Team)
			throw(err)
			notifications := mylog.MatchConfig(configs)
			for _, notification := range notifications {
				err = myBreaker.Do(GlobalConfig.QueueConfig[1].QConnectionString, GlobalConfig.QueueConfig[1].QName, notification)
				if err != nil {
					err = SendMessage(GlobalConfig.QueueConfig[3].QConnectionString, GlobalConfig.QueueConfig[3].QName, notification)
					throw(err)
				}
			}
			err = message.Ack(true)
			throw(err)
		}
	}()
	go func() {
		for upd := range updates {
			myUpd := ConfigUPD{}
			err = json.Unmarshal(upd.Body, &myUpd)
			throw(err)
			if myUpd.UpdateType != "Delete" {
				err := setConfig(myUpd.TeamConfig)
				throw(err)
			} else {
				err := removeConfig(myUpd.TeamConfig)
				throw(err)
			}
			err = upd.Ack(true)
			throw(err)
		}
	}()
	<-forever
}
