package main

import (
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
	PQServerAddress := os.Getenv("pnotqserveraddress")
	SQName := os.Getenv("snotqname")
	SQConStr := os.Getenv("snotqconnectionstringpath")
	SQServerAddress := os.Getenv("snotqserveraddress")
	PLogQName := os.Getenv("plogqname")
	PLogQConStr := os.Getenv("plogqconnectionstringpath")
	PLogQServerAddress := os.Getenv("plogqserveraddress")
	SLogQName := os.Getenv("slogqname")
	SLogQConStr := os.Getenv("slogqconnectionstringpath")
	SLogQServerAddress := os.Getenv("slogqserveraddress")
	StatusQName := os.Getenv("statusqname")
	StatusQConStr := os.Getenv("statusqconnectionstringpath")
	StatusQServerAddress := os.Getenv("statusqserveraddress")
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
	if PQServerAddress == "" {
		return fmt.Errorf("cannot get pnotqserveraddress environment variable")
	}
	bytes, err = os.ReadFile(PQConStr)
	if err != nil {
		return err
	}
	pnotqcs := strings.Split(string(bytes), "\n")[0]
	pqcs := fmt.Sprintf("amqp://%s@%s", pnotqcs, PQServerAddress)
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
	if SQServerAddress == "" {
		return fmt.Errorf("cannot get snotqserveraddress environment variable")
	}
	sqnotcs := strings.Split(string(bytes), "\n")[0]
	sqcs := fmt.Sprintf("amqp://%s@%s", sqnotcs, SQServerAddress)
	if SQName == "" {
		return fmt.Errorf("cannot get snotqname environment variable")
	}
	if PLogQName == "" {
		return fmt.Errorf("cannot get plogqname environment variable")
	}
	if PLogQConStr == "" {
		return fmt.Errorf("cannot get plogqconnectionstringPath environment variable")
	}
	if PLogQServerAddress == "" {
		return fmt.Errorf("cannot get plogqserveraddress environment variable")
	}
	if SLogQName == "" {
		return fmt.Errorf("cannot get slogqname environment variable")
	}
	if SLogQConStr == "" {
		return fmt.Errorf("cannot get slogqconnectionstringPath environment variable")
	}
	if SLogQServerAddress == "" {
		return fmt.Errorf("cannot get slogqserveraddress environment variable")
	}
	bytes, err = os.ReadFile(PLogQConStr)
	if err != nil {
		return err
	}
	plogqpass := strings.Split(string(bytes), "\n")[0]
	plogqcs := fmt.Sprintf("amqp://%s@%s", plogqpass, PLogQServerAddress)
	if StatusQName == "" {
		return fmt.Errorf("cannot get statusqname environment variable")
	}
	bytes, err = os.ReadFile(SLogQConStr)
	if err != nil {
		return err
	}
	slogqpass := strings.Split(string(bytes), "\n")[0]
	slogqcs := fmt.Sprintf("amqp://%s@%s", slogqpass, SLogQServerAddress)
	if StatusQConStr == "" {
		return fmt.Errorf("cannot get statusqconnectionstringPath environment variable")
	}
	if StatusQServerAddress == "" {
		return fmt.Errorf("cannot get statusqserveraddress environment variable")
	}
	bytes, err = os.ReadFile(StatusQConStr)
	if err != nil {
		return err
	}
	statusqpass := strings.Split(string(bytes), "\n")[0]
	statusqcs := fmt.Sprintf("amqp://%s@%s", statusqpass, StatusQServerAddress)
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
	// Creating instance of FileManager for Primary Log Queue
	FMPLogQ := GetFileManagerDefaultInstance(GlobalConfig.QueueConfig[0])
	PRetrier := GetRetrierOverloadInstance(FMPLogQ.StartReceiveMessage)
	Pmessages := PRetrier.Do()
	// Creating instance of FileManager for Secondary Log Queue
	FMSLogQ := GetFileManagerDefaultInstance(GlobalConfig.QueueConfig[1])
	SRetrier := GetRetrierOverloadInstance(FMSLogQ.StartReceiveMessage)
	Smessages := SRetrier.Do()
	// Creating instance of FileManager for Primary Notification Queue
	FMPNotQ := GetFileManagerDefaultInstance(GlobalConfig.QueueConfig[2])
	// Creating instance of FileManager for Secondary Notification Queue
	FMSNotQ := GetFileManagerDefaultInstance(GlobalConfig.QueueConfig[3])
	// Creating instance of Connection Manager for Primary Queue with Primary File Manager
	myPBreaker := GetBreakerOverloadInstance(FMPNotQ.SendMessage)
	// Creating instance of Connection Manager for Secondary Queue with Secondary File Manager
	mySBreaker := GetBreakerOverloadInstance(FMSNotQ.SendMessage)
	// Creating instance of FileManager for Secondary Notification Queue
	FMStatusQ := GetFileManagerDefaultInstance(GlobalConfig.QueueConfig[4])
	forever := make(chan bool)
	go func() {
		for message := range Pmessages {
			err := FMSNotQ.ProcessMessage(message, myPBreaker)
			throw(err)
		}
	}()
	go func() {
		for message := range Smessages {
			err := FMPLogQ.ProcessMessage(message, mySBreaker)
			throw(err)
		}
	}()
	go func() {
		URetrier := GetRetrierOverloadInstance(FMStatusQ.StartReceiveMessage)
		updates := URetrier.Do()
		for upd := range updates {
			err := FMStatusQ.ProcessConfig(upd)
			throw(err)
		}
	}()
	<-forever
}
