package main

import (
	"regexp"
	"time"
)

type TeamConfig struct {
	Name                  string   `bson:"Name" json:"Name"`
	Team                  string   `bson:"Team" json:"Team"`
	LogPattern            string   `bson:"LogPattern" json:"LogPattern"`
	LogSeverity           string   `bson:"LogSeverity" json:"LogSeverity"`
	NotificationMethod    string   `bson:"NotificationMethod" json:"NotificationMethod"`
	LogLogic              string   `bson:"LogLogic" json:"LogLogic"`
	NotificationRecipient []string `bson:"NotificationRecipient" json:"NotificationRecipient"`
	HoldTime              int      `bson:"HoldTime" json:"HoldTime"`
	RetryCount            int      `bson:"RetryCount" json:"RetryCount"`
}

type Notification struct {
	Log                   string
	NotificationMethod    string
	NotificationRecipient string
	HoldTime              int
	RetryCount            int
	Retried               int
}

type ConfigUPD struct {
	TeamConfig
	UpdateType string
	UpdateTime time.Time
}

type Log struct {
	Team     string
	Severity string
	Log      string
}

func (l *Log) MatchConfig(TeamConfigs []TeamConfig) []Notification {
	var notifications []Notification
	for _, conf := range TeamConfigs {
		isExpMatches := false
		if conf.LogPattern == "" {
			isExpMatches = false
		} else {
			ex := regexp.MustCompile(conf.LogPattern)
			isExpMatches = ex.MatchString(l.Log)
		}
		isMatched := false
		isAnd := conf.LogLogic == "AND"
		isOr := conf.LogLogic == "OR"
		isSeverityMatches := l.Severity == conf.LogSeverity
		if isOr {
			if isExpMatches || isSeverityMatches {
				isMatched = true
			}
		} else if isSeverityMatches {
			if isAnd && isExpMatches {
				isMatched = true
			} else if !isAnd {
				isMatched = true
			}
		}
		if isMatched {
			for _, recipe := range conf.NotificationRecipient {
				note := Notification{
					Log:                   l.Log,
					NotificationMethod:    conf.NotificationMethod,
					NotificationRecipient: recipe,
					HoldTime:              conf.HoldTime,
					RetryCount:            conf.RetryCount,
				}
				notifications = append(notifications, note)
			}
		}
	}
	return notifications
}
