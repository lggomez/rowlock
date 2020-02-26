package rowlock

import (
	"sync/atomic"
	"time"

	"github.com/beevik/timerqueue"
)

type unlockScheduler struct {
	queue     *timerqueue.Queue
	ticker    *time.Ticker
	timerChan chan timerValue
	running   *int64
}

func NewUnlockScheduler() *unlockScheduler {
	f := func(s int64) *int64 {
		return &s
	}
	return &unlockScheduler{
		queue:     timerqueue.New(),
		ticker:    time.NewTicker(500 * time.Microsecond),
		timerChan: make(chan timerValue, 1000),
		running:   f(0),
	}
}

func (l *unlockScheduler) Run() {
	if !atomic.CompareAndSwapInt64(l.running, 0, 1) {
		return
	}
	go l.worker()
}

func (l *unlockScheduler) worker() {
	for {
		select {
		case <-l.ticker.C:
			{
				l.queue.Advance(time.Now())
			}
		case msg := <-l.timerChan:
			{
				if msg.lock.tm == nil {
					now := time.Now()
					msg.lock.tm = &now
				}
				newTm := (*msg.lock.tm).Add(msg.ttl)
				msg.lock.tm = &newTm
				l.queue.Schedule(msg.lock, *msg.lock.tm)
			}
		}
	}
}

type timerValue struct {
	lock *SafeLock
	ttl  time.Duration
}

func (l *unlockScheduler) ScheduleTimer(lock *SafeLock, ttl time.Duration) {
	// Serialize push operations into timerqueue to ensure goroutine safety
	l.timerChan <- timerValue{
		lock: lock,
		ttl:  ttl,
	}
}
