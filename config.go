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

func (l *Log) MatchConfig(tc []TeamConfig) []Notification {
	var note []Notification
	for _, conf := range tc {
		if conf.LogLogic == "OR" {
			ex := regexp.MustCompile(conf.LogPattern)
			if ex.MatchString(l.Log) || l.Severity == conf.LogSeverity {
				for _, rec := range conf.NotificationRecipient {
					nt := Notification{
						Log:                   l.Log,
						NotificationMethod:    conf.NotificationMethod,
						NotificationRecipient: rec,
						HoldTime:              conf.HoldTime,
						RetryCount:            conf.RetryCount,
					}
					note = append(note, nt)
				}
			}
		} else if conf.LogLogic == "AND" {
			ex := regexp.MustCompile(conf.LogPattern)
			if ex.MatchString(l.Log) && l.Severity == conf.LogSeverity {
				for _, rec := range conf.NotificationRecipient {
					nt := Notification{
						Log:                   l.Log,
						NotificationMethod:    conf.NotificationMethod,
						NotificationRecipient: rec,
						HoldTime:              conf.HoldTime,
						RetryCount:            conf.RetryCount,
					}
					note = append(note, nt)
				}
			}
		} else if conf.LogLogic == "" {
			if l.Severity == conf.LogSeverity {
				for _, rec := range conf.NotificationRecipient {
					nt := Notification{
						Log:                   l.Log,
						NotificationMethod:    conf.NotificationMethod,
						NotificationRecipient: rec,
						HoldTime:              conf.HoldTime,
						RetryCount:            conf.RetryCount,
					}
					note = append(note, nt)
				}
			}
		}
	}
	return note
}
