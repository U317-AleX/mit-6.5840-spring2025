package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"github.com/google/uuid"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	version rpc.Tversion
	l string
	id uuid.UUID
	// true for this thread own the lock, false for the opposite
	isOwn bool
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.id = uuid.New()
	lk.l = l
	lk.version = 0
	lk.isOwn = false
	return lk
}

// use "" sign for unlock state, "_" for lock state
func (lk *Lock) UnlockState() (string) {
	return ""
}

func (lk *Lock) LockState() (string) {
	return lk.id.String()
}

func (lk *Lock) Acquire() {
	// Your code here
	if lk.isOwn {
		panic("lock already owned")
	}

	// use while-retry pattern
	for {
		// test if the lock is free
		state, vers, _ := lk.ck.Get(lk.l)
		lk.version = vers

		if state == lk.UnlockState() {
			// try to race for the lock
			err := lk.ck.Put(lk.l, lk.LockState(), lk.version)

			// fetch the lock if the lock is free and the version is correct
			if err == rpc.OK {
				lk.isOwn = true
				lk.version++
				return
			}

			if err == rpc.ErrMaybe {
				// check whether the id match
				state, _, _ = lk.ck.Get(lk.l)
				if state == lk.id.String() {
					lk.isOwn = true
					lk.version++
					return
				}
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	if !lk.isOwn {
		panic("lock not owned")
	}

	// use while-retry pattern
	for {
		// try to release the lock
		err := lk.ck.Put(lk.l, lk.UnlockState(), lk.version)

		if err == rpc.OK {
			lk.isOwn = false
			lk.version++
			return
		}

		if err == rpc.ErrMaybe {
			// check whether the id match
			state, _, _ := lk.ck.Get(lk.l)
			if state != lk.id.String() {
				lk.isOwn = false
				lk.version++
				return
			}
		}
	}
}
