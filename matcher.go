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
	PQName := os.Getenv("pnotqname")
	PQConStr := os.Getenv("pnotqconnectionstringpath")
	SQName := os.Getenv("snotqname")
	SQConStr := os.Getenv("snotqconnectionstringpath")
	PLogQName := os.Getenv("plogqname")
	PLogQConStr := os.Getenv("plogqconnectionstringpath")
	SLogQName := os.Getenv("slogqname")
	SLogQConStr := os.Getenv("slogqconnectionstringpath")
	StatusQName := os.Getenv("statusqname")
	StatusQConStr := os.Getenv("statusqconnectionstringpath")
	if rdb == "" {
		return fmt.Errorf("cannot get dbcs environment variable")
	}
	bytes, err := os.ReadFile(rdb)
	if err != nil {
		return err
	}
	rdbcs := strings.Split(string(bytes), "\n")[0]
	if PQConStr == "" {
		return fmt.Errorf("cannot get pnotqconnectionstringpath environment variable")
	}
	bytes, err = os.ReadFile(PQConStr)
	if err != nil {
		return err
	}
	pqcs := strings.Split(string(bytes), "\n")[0]
	if PQName == "" {
		return fmt.Errorf("cannot get pnotqname environment variable")
	}
	if SQConStr == "" {
		return fmt.Errorf("cannot get snotqconnectionstringpath environment variable")
	}
	bytes, err = os.ReadFile(SQConStr)
	if err != nil {
		return err
	}
	sqcs := strings.Split(string(bytes), "\n")[0]
	if SQName == "" {
		return fmt.Errorf("cannot get snotqname environment variable")
	}
	if PLogQName == "" {
		return fmt.Errorf("cannot get plogqname environment variable")
	}
	if PLogQConStr == "" {
		return fmt.Errorf("cannot get plogqconnectionstringPath environment variable")
	}
	if SLogQName == "" {
		return fmt.Errorf("cannot get slogqname environment variable")
	}
	if SLogQConStr == "" {
		return fmt.Errorf("cannot get slogqconnectionstringPath environment variable")
	}
	bytes, err = os.ReadFile(PLogQConStr)
	if err != nil {
		return err
	}
	plogqcs := strings.Split(string(bytes), "\n")[0]
	if StatusQName == "" {
		return fmt.Errorf("cannot get statusqname environment variable")
	}
	bytes, err = os.ReadFile(SLogQConStr)
	if err != nil {
		return err
	}
	slogqcs := strings.Split(string(bytes), "\n")[0]
	if StatusQConStr == "" {
		return fmt.Errorf("cannot get statusqconnectionstringPath environment variable")
	}
	bytes, err = os.ReadFile(StatusQConStr)
	if err != nil {
		return err
	}
	statusqcs := strings.Split(string(bytes), "\n")[0]
	q1 := qconfig{QConnectionString: plogqcs, QName: PLogQName}
	q2 := qconfig{QConnectionString: slogqcs, QName: SLogQName}
	q3 := qconfig{QConnectionString: pqcs, QName: PQName}
	q4 := qconfig{QConnectionString: sqcs, QName: SQName}
	q5 := qconfig{QConnectionString: statusqcs, QName: StatusQName}
	qconf := []qconfig{q1, q2, q3, q4, q5}
	wconf := webconfig{QueueConfig: qconf, Cache: rdbcs}
	GlobalConfig = wconf
	return nil
}

func main() {
	err := getEnvVars()
	throw(err)
	PRetrier := Retrier{}
	PRetrier.New(ReceiveMessage)
	Pmessages, err := PRetrier.Do(GlobalConfig.QueueConfig[0].QConnectionString, GlobalConfig.QueueConfig[0].QName)
	SRetrier := Retrier{}
	SRetrier.New(ReceiveMessage)
	Smessages, err := SRetrier.Do(GlobalConfig.QueueConfig[1].QConnectionString, GlobalConfig.QueueConfig[1].QName)
	throw(err)
	updates, err := ReceiveMessage(GlobalConfig.QueueConfig[4].QConnectionString, GlobalConfig.QueueConfig[4].QName)
	throw(err)
	myBreaker := Breaker{}
	myBreaker.New(SendMessage)
	forever := make(chan bool)
	go func() {
		for message := range Pmessages {
			mylog := Log{}
			err = json.Unmarshal(message.Body, &mylog)
			throw(err)
			configs, err := getConfig(mylog.Team)
			throw(err)
			notifications := mylog.MatchConfig(configs)
			for _, notification := range notifications {
				err = myBreaker.Do(GlobalConfig.QueueConfig[2].QConnectionString, GlobalConfig.QueueConfig[2].QName, notification)
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
		for message := range Smessages {
			mylog := Log{}
			err = json.Unmarshal(message.Body, &mylog)
			throw(err)
			configs, err := getConfig(mylog.Team)
			throw(err)
			notifications := mylog.MatchConfig(configs)
			for _, notification := range notifications {
				err = myBreaker.Do(GlobalConfig.QueueConfig[2].QConnectionString, GlobalConfig.QueueConfig[2].QName, notification)
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
