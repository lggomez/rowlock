package rowlock_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lggomez/rowlock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRowLock_Mutex(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	lock := rowlock.NewRowLock(rowlock.MutexNewLocker)
	key1 := "key1"

	lock.Lock(key1)

	assert.True(t, lock.IsLocked(key1), key1)

	lock.Unlock(key1)

	assert.False(t, lock.IsLocked(key1), key1)

	// test double unlock
	done := make(chan struct{}, 1)
	go func() {
		lock.Unlock(key1)
		done <- struct{}{}
	}()

	select {
	case <-done:
		t.Errorf("unexpected return from infinite loop")
	case <-time.After(1 * time.Second):
		return
	}
}

func TestRowLock_RMutex(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	lock := rowlock.NewRowLock(rowlock.RWMutexNewLocker)
	key1 := "key1"

	lock.RLock(key1)

	assert.True(t, lock.IsLocked(key1), key1)

	lock.RUnlock(key1)

	assert.False(t, lock.IsLocked(key1), key1)

	// test double unlock
	done := make(chan struct{}, 1)
	go func() {
		lock.RUnlock(key1)
		done <- struct{}{}
	}()

	select {
	case <-done:
		t.Errorf("unexpected return from infinite loop")
	case <-time.After(1 * time.Second):
		return
	}
}

func TestRowLockTTL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	lock := rowlock.NewRowLock(rowlock.MutexNewLocker)
	key1 := "key1"
	key2 := "key2"
	key3 := "key3"
	keyunknown := "unknown"

	lock.LockWithTTL(key1, 10*time.Millisecond)
	lock.LockWithTTL(key2, 50*time.Millisecond)
	lock.LockWithTTL(key3, 10*time.Millisecond)

	time.Sleep(5 * time.Millisecond)

	lock.KeepAlive(key3, 10*time.Millisecond)
	assert.True(t, lock.IsLocked(key3), key3)
	lock.KeepAlive(key3, 10*time.Millisecond)
	assert.True(t, lock.IsLocked(key3), key3)
	lock.KeepAlive(keyunknown, 20*time.Millisecond)
	assert.True(t, lock.IsLocked(keyunknown), keyunknown)

	assert.True(t, lock.IsLocked(key1), key1)
	assert.True(t, lock.IsLocked(key2), key2)
	assert.True(t, lock.IsLocked(key2), key2)

	time.Sleep(15 * time.Millisecond)

	assert.False(t, lock.IsLocked(key1), key1)
	assert.True(t, lock.IsLocked(key2), key2)
	assert.True(t, lock.IsLocked(key3), key3)

	// no panic
	lock.Unlock(key1)
	lock.Unlock(key1)

	lock.Unlock(key2)

	assert.False(t, lock.IsLocked(key2), key2)

	// key3 expiration after keepalive postponed unlock
	time.Sleep(15 * time.Millisecond)
	assert.False(t, lock.IsLocked(key3), key3)
}

func TestRowRLockTTL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	lock := rowlock.NewRowLock(rowlock.MutexNewLocker)
	key1 := "key1"
	key2 := "key2"
	key3 := "key3"
	keyunknown := "unknown"

	lock.RLockWithTTL(key1, 10*time.Millisecond)
	lock.RLockWithTTL(key2, 50*time.Millisecond)
	lock.RLockWithTTL(key3, 10*time.Millisecond)

	time.Sleep(5 * time.Millisecond)

	lock.RKeepAlive(key3, 10*time.Millisecond)
	assert.True(t, lock.IsLocked(key3), key3)
	lock.RKeepAlive(key3, 10*time.Millisecond)
	assert.True(t, lock.IsLocked(key3), key3)
	lock.RKeepAlive(keyunknown, 20*time.Millisecond)
	assert.True(t, lock.IsLocked(keyunknown), keyunknown)

	assert.True(t, lock.IsLocked(key1), key1)
	assert.True(t, lock.IsLocked(key2), key2)
	assert.True(t, lock.IsLocked(key2), key2)

	time.Sleep(15 * time.Millisecond)

	assert.False(t, lock.IsLocked(key1), key1)
	assert.True(t, lock.IsLocked(key2), key2)
	assert.True(t, lock.IsLocked(key3), key3)

	// no panic
	lock.RUnlock(key1)
	lock.RUnlock(key1)

	lock.RUnlock(key2)

	assert.False(t, lock.IsLocked(key2), key2)

	// key3 expiration after keepalive postponed unlock
	time.Sleep(15 * time.Millisecond)
	assert.False(t, lock.IsLocked(key3), key3)
}

func TestRowLockAsync(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	l := rowlock.NewRowLock(rowlock.MutexNewLocker)
	key1 := "key1"
	key2 := "key2"

	short := time.Millisecond * 10
	long := time.Millisecond * 100
	longer := time.Millisecond * 150

	var locksWg sync.WaitGroup
	locksWg.Add(2)

	go func() {
		l.Lock(key1)
		locksWg.Done()
		defer l.Unlock(key1)
		time.Sleep(long)
	}()

	go func() {
		l.Lock(key2)
		locksWg.Done()
		defer l.Unlock(key2)
		time.Sleep(longer)
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		// wait for keys to be locked
		started := time.Now()
		locksWg.Wait()
		time.Sleep(short)

		l.Lock(key1)
		elapsed := time.Since(started)

		defer l.Unlock(key1)
		t.Logf("elapsed time: %v", elapsed)
		if elapsed < long || elapsed > longer {
			t.Errorf(
				"lock wait time should be between %v and %v, actual %v",
				long,
				longer,
				elapsed,
			)
		}
	}()

	wg.Wait()
}

func TestRowRLockAsync(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	l := rowlock.NewRowLock(rowlock.RWMutexNewLocker)
	key1 := "key1"
	key2 := "key2"

	short := time.Millisecond * 10
	long := time.Millisecond * 100
	longer := time.Millisecond * 150

	var locksWg sync.WaitGroup
	locksWg.Add(2)

	go func() {
		l.RLock(key1)
		locksWg.Done()
		defer l.RUnlock(key1)
		time.Sleep(long)
	}()

	go func() {
		l.RLock(key2)
		locksWg.Done()
		defer l.RUnlock(key2)
		time.Sleep(longer)
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		// wait for keys to be locked
		started := time.Now()
		locksWg.Wait()
		time.Sleep(short)

		l.RLock(key1)
		elapsed := time.Since(started)

		defer l.RUnlock(key1)
		t.Logf("elapsed time: %v", elapsed)
		if elapsed < long || elapsed > longer {
			t.Errorf(
				"lock wait time should be between %v and %v, actual %v",
				long,
				longer,
				elapsed,
			)
		}
	}()

	wg.Wait()
}

func TestUseNonRWForReadLock(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	lock := rowlock.NewRowLock(rowlock.MutexNewLocker)
	key1 := "key1"
	key2 := "key2"

	short := time.Millisecond * 10
	long := time.Millisecond * 100
	longer := time.Millisecond * 150

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		lock.RLock(key1)
		defer lock.RUnlock(key1)
		time.Sleep(long)
	}()

	go func() {
		defer wg.Done()
		lock.RLock(key2)
		defer lock.RUnlock(key2)
		time.Sleep(longer)
	}()

	go func() {
		defer wg.Done()
		started := time.Now()
		time.Sleep(short)
		lock.RLock(key1)
		defer lock.RUnlock(key1)
		elapsed := time.Since(started)
		t.Logf("elapsed time: %v", elapsed)
		if elapsed < long || elapsed > longer {
			t.Errorf(
				"lock wait time should be between %v and %v, actual %v",
				long,
				longer,
				elapsed,
			)
		}
	}()

	wg.Wait()
}

func TestMultiAcquireSingleMutex(t *testing.T) {
	lock := rowlock.NewRowLock(rowlock.MutexNewLocker)
	key1 := "key1"
	key2 := "key2"

	var wg sync.WaitGroup

	readKeys := []string{key1, key1, key2, key2}
	readSleeps := []time.Duration{
		time.Millisecond * 250,
		time.Millisecond * 200,
		time.Millisecond * 350,
		time.Millisecond * 300,
	}
	wg.Add(len(readKeys))

	writeKeys := []string{key1, key1, key2, key2}
	writeSleeps := []time.Duration{
		time.Millisecond * 350,
		time.Millisecond * 150,
		time.Millisecond * 450,
		time.Millisecond * 200,
	}
	wg.Add(len(writeKeys))

	// Read locks
	for i := range readKeys {
		go func(key string, sleep time.Duration) {
			defer wg.Done()
			time.Sleep(sleep)
			lock.Lock(key)
			require.True(t, lock.IsLocked(key), key)
			defer lock.Unlock(key)
			time.Sleep(sleep)
		}(readKeys[i], readSleeps[i])
	}

	// Write locks
	for i := range writeKeys {
		go func(key string, sleep time.Duration) {
			defer wg.Done()
			time.Sleep(sleep)
			lock.Lock(key)
			require.True(t, lock.IsLocked(key), key)
			defer lock.Unlock(key)
			time.Sleep(sleep)
		}(writeKeys[i], writeSleeps[i])
	}

	wg.Wait()
}

func TestMultiAcquireRWMutex(t *testing.T) {
	lock := rowlock.NewRowLock(rowlock.RWMutexNewLocker)
	key1 := "key1"
	key2 := "key2"

	var wg sync.WaitGroup

	readKeys := []string{key1, key1, key2, key2}
	readSleeps := []time.Duration{
		time.Millisecond * 250,
		time.Millisecond * 200,
		time.Millisecond * 350,
		time.Millisecond * 300,
	}
	wg.Add(len(readKeys))

	writeKeys := []string{key1, key1, key2, key2}
	writeSleeps := []time.Duration{
		time.Millisecond * 350,
		time.Millisecond * 150,
		time.Millisecond * 450,
		time.Millisecond * 200,
	}
	wg.Add(len(writeKeys))

	// Read locks
	for i := range readKeys {
		go func(key string, sleep time.Duration) {
			defer wg.Done()
			time.Sleep(sleep)
			lock.RLock(key)
			require.True(t, lock.IsLocked(key), key)
			defer lock.RUnlock(key)
			time.Sleep(sleep)
		}(readKeys[i], readSleeps[i])
	}

	// Write locks
	for i := range writeKeys {
		go func(key string, sleep time.Duration) {
			defer wg.Done()
			time.Sleep(sleep)
			lock.Lock(key)
			require.True(t, lock.IsLocked(key), key)
			defer lock.Unlock(key)
			time.Sleep(sleep)
		}(writeKeys[i], writeSleeps[i])
	}

	wg.Wait()
}

func BenchmarkLockUnlock(b *testing.B) {
	var numRows = []int{10, 100, 1000}
	var newLockerMap = map[string]rowlock.NewLocker{
		"Mutex":   rowlock.MutexNewLocker,
		"RWMutex": rowlock.RWMutexNewLocker,
	}

	for _, n := range numRows {
		b.Run(
			fmt.Sprintf("%d", n),
			func(b *testing.B) {
				rows := make([]int, n)
				for i := 0; i < n; i++ {
					rows[i] = i
				}
				for label, newLocker := range newLockerMap {
					b.Run(
						label,
						func(b *testing.B) {
							rl := rowlock.NewRowLock(newLocker)

							b.Run(
								"LockUnlock",
								func(b *testing.B) {
									for i := 0; i < b.N; i++ {
										row := rows[i%n]
										rl.Lock(fmt.Sprintf("%v", row))
										rl.Unlock(fmt.Sprintf("%v", row))
									}
								},
							)

							b.Run(
								"RLockRUnlock",
								func(b *testing.B) {
									for i := 0; i < b.N; i++ {
										row := rows[i%n]
										rl.RLock(fmt.Sprintf("%v", row))
										rl.RUnlock(fmt.Sprintf("%v", row))
									}
								},
							)
						},
					)
				}
			},
		)
	}
}
