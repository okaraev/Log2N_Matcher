package main

import (
	"errors"
	"log"
	"time"
)

type Retrier struct {
	Status      string        // Retrier Status
	HoldTime    time.Duration // Time duration after which Operation will retry. If Operation fails hold time multiplies until 360 seconds
	MaxHoldTime time.Duration
	Operation   func() (<-chan interface{}, error)
}

func GetRetrierOverloadInstance(Func func() (<-chan interface{}, error)) Retrier {
	r := Retrier{}
	r.HoldTime = 10 * time.Second
	r.MaxHoldTime = 360 * time.Second
	r.Status = "Closed"
	r.Operation = Func
	return r
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

func (r *Retrier) Do() <-chan interface{} {
	result, err := r.Operation()
	commonReceiver := make(chan interface{})
	if err != nil {
		r.Open()
		log.Println(err)
	}
	if r.isClosed() {
		// if status is success starting routine to receive messages
		go func() {
			for message := range result {
				commonReceiver <- message
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
				retryResult, err := r.Operation()
				if err != nil {
					log.Println(err)
					r.Multiply()
				} else {
					// if retry is success setting status to success and starting routine to receive messages
					r.Close()
					go func() {
						for message := range retryResult {
							commonReceiver <- message
						}
						// Ending routine when error happened and setting status to fail
						r.Open()
					}()
				}
			}
		}
	}()
	return commonReceiver
}

type Breaker struct {
	Status           string        // Breaker Status
	FailCount        int           // Failed operations' count
	LastFail         time.Time     // Succeeded operations' count
	FailThreshold    int           // Failed operations threshold
	SuccessThreshold time.Duration // Time duration in which all operations must be succeeded after that FailCount will reset and Status will change to 'Closed'
	OpenThreshold    time.Duration // Time duration after which Status will change to 'HalfOpen'
	Operation        func(message interface{}) error
}

func GetBreakerOverloadInstance(Func func(message interface{}) error) Breaker {
	b := Breaker{}
	b.Status = "Closed"
	b.OpenThreshold = 30 * time.Second
	b.FailCount = 0
	b.FailThreshold = 3
	b.LastFail = time.Now()
	b.SuccessThreshold = 1 * time.Minute
	b.Operation = Func
	return b
}

func (b *Breaker) Open() {
	go func() {
		b.Status = "Open"
		time.Sleep(b.OpenThreshold)
		b.Status = "HalfOpen"
	}()
}

func (b *Breaker) Do(message interface{}) error {
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
	err := b.Operation(message)
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
