package rowlock

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	locked   int64 = 1
	unlocked int64 = 0
)

// NewLocker defines a type of function that can be used to create a new Locker.
type NewLocker func() SafeLocker

// Row is the type of a row lock
type Row = string

// Make sure that sync.RWMutex is compatible with SafeLocker interface.
var _ SafeLocker = (*sync.RWMutex)(nil)

// SafeLocker is the abstracted interface of sync.RWMutex.
type SafeLocker interface {
	sync.Locker
	RLocker() sync.Locker
}

type SafeLock struct {
	sync.Locker
	lock     sync.Locker
	isLocked *int64
	tm       *time.Time
}

func NewSafeLock(lock sync.Locker) SafeLocker {
	f := func(s int64) *int64 {
		return &s
	}
	return &SafeLock{
		lock:     lock,
		isLocked: f(unlocked),
	}
}

func (s *SafeLock) Lock() {
LU:
	if !atomic.CompareAndSwapInt64(s.isLocked, unlocked, locked) {
		runtime.Gosched()
		goto LU
	}
	s.lock.Lock()
}

func (s *SafeLock) Unlock() {
RU:
	if !atomic.CompareAndSwapInt64(s.isLocked, locked, unlocked) {
		// If its TTL is already past and it was unlocked from scheduler, return
		if s.tm != nil && s.tm.Before(time.Now()) {
			return
		}
		// deadlock/unbounded loop: we are trying to unlock an already unlocked lock
		// which had no TTL set
		runtime.Gosched()
		goto RU
	}
	s.lock.Unlock()
}

func (s *SafeLock) IsLocked() bool {
	return atomic.LoadInt64(s.isLocked) == locked
}

func (s *SafeLock) RLocker() sync.Locker {
	return s
}

func (s *SafeLock) OnTimer(t time.Time) {
	if s.tm.After(t) {
		// Lock is being kept alive and this timer is stale
		// This should be unreachable since all timers are indexed
		// and updated by reference
		return
	}
	if s.IsLocked() {
		s.Unlock()
	}
}

// MutexNewLocker is a NewLocker using sync.Mutex.
func MutexNewLocker() SafeLocker {
	return NewSafeLock(new(sync.Mutex))
}

// RWMutexNewLocker is a NewLocker using sync.RWMutex.
func RWMutexNewLocker() SafeLocker {
	return NewSafeLock(new(sync.RWMutex))
}

// RowLock defines a set of locks.
//
// When you do Lock/Unlock operations, you don't do them on a global scale.
// Instead, a Lock/Unlock operation is operated on a given row.
//
// If NewLocker returns an implementation of SafeLocker in NewRowLock,
// the RowLock can be locked separately for read in RLock and RUnlock functions.
// Otherwise, RLock is the same as Lock and RUnlock is the same as Unlock.
type RowLock struct {
	locks      sync.Map
	lockerPool sync.Pool
	sched      *unlockScheduler
}

// NewRowLock creates a new RowLock with the given NewLocker.
func NewRowLock(f NewLocker) *RowLock {
	rl := &RowLock{
		lockerPool: sync.Pool{
			New: func() interface{} {
				return f()
			},
		},
		sched: NewUnlockScheduler(),
	}
	return rl
}

// Lock locks a row.
//
// If this is a new row,
// a new locker will be created using the NewLocker specified in NewRowLock.
func (rl *RowLock) Lock(row Row) {
	rl.getLocker(row).Lock()
}

// Unlock unlocks a row.
func (rl *RowLock) Unlock(row Row) {
	rl.getLocker(row).Unlock()
}

// RLock locks a row for read.
//
// It only works as expected when NewLocker specified in NewRowLock returns an
// implementation of SafeLocker. Otherwise, it's the same as Lock.
func (rl *RowLock) RLock(row Row) {
	rLocker := rl.getRLocker(row)
	rLocker.Lock()
}

// RUnlock unlocks a row for read.
//
// It only works as expected when NewLocker specified in NewRowLock returns an
// implementation of SafeLocker. Otherwise, it's the same as Unlock.
func (rl *RowLock) RUnlock(row Row) {
	rLocker := rl.getRLocker(row)
	rLocker.Unlock()
}

// Lock locks a row.
//
// If this is a new row,
// a new locker will be created using the NewLocker specified in NewRowLock.
func (rl *RowLock) LockWithTTL(row Row, ttl time.Duration) {
	// Lazy init of unlock scheduler
	rl.sched.Run()

	rl.Lock(row)
	rl.scheduleTimer(row, ttl)
}

// RLock locks a row for read.
//
// It only works as expected when NewLocker specified in NewRowLock returns an
// implementation of SafeLocker. Otherwise, it's the same as Lock.
func (rl *RowLock) RLockWithTTL(row Row, ttl time.Duration) {
	// Lazy init of unlock scheduler
	rl.sched.Run()

	rl.RLock(row)
	rl.scheduleTimer(row, ttl)
}

func (rl *RowLock) scheduleTimer(row Row, ttl time.Duration) {
	sl := rl.getLocker(row).(*SafeLock)
	rl.sched.ScheduleTimer(sl, ttl)
}

func (rl *RowLock) KeepAlive(row Row, ttl time.Duration) {
	// Lazy init of unlock scheduler
	rl.sched.Run()

	sl := rl.getLocker(row).(*SafeLock)
	if !sl.IsLocked() {
		rl.LockWithTTL(row, ttl)
		return
	}

	// Re-enqueue unlock event
	rl.sched.ScheduleTimer(sl, ttl)
}

func (rl *RowLock) RKeepAlive(row Row, ttl time.Duration) {
	// Lazy init of unlock scheduler
	rl.sched.Run()

	sl := rl.getRLocker(row).(*SafeLock)
	if !sl.IsLocked() {
		rl.RLockWithTTL(row, ttl)
		return
	}

	// Re-enqueue unlock event
	rl.sched.ScheduleTimer(sl, ttl)
}

// getLocker returns the lock for the given row.
//
// If this is a new row,
// a new locker will be created using the NewLocker specified in NewRowLock.
func (rl *RowLock) getLocker(row Row) sync.Locker {
	// get existing or new Mutex/RMutex instance from pool
	newLocker := rl.lockerPool.Get()

	// update map if necessary with instance for given row
	locker, loaded := rl.locks.LoadOrStore(row, newLocker)
	if loaded {
		rl.lockerPool.Put(newLocker)
	}

	return locker.(sync.Locker)
}

// getRLocker returns the lock for read for the given row.
//
// If this is a new row,
// a new locker will be created using the NewLocker specified in NewRowLock.
//
// If NewLocker specified in NewRowLock returns a locker that didn't implement
// GetRLocker, the locker itself will be returned instead.
func (rl *RowLock) getRLocker(row Row) sync.Locker {
	locker := rl.getLocker(row)

	return locker.(SafeLocker).RLocker()
}

func (rl *RowLock) IsLocked(row Row) bool {
	locker := rl.getRLocker(row)

	return locker.(*SafeLock).IsLocked()
}
