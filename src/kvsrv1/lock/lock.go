package lock

import (

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
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
	return "_"
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
		}
	}
	
	// here is the code don not use while-retry pattern

	// // wait for the lock to be released
	// for state, vers, _ := lk.ck.Get(lk.l); state == lk.LockState(); state, vers, _ = lk.ck.Get(lk.l) {
	// 	log.Printf("key: " + lk.l + " state: " + state + " waiting for lock to be released")
	// 	lk.version = vers
	// 	log.Print("version: " + strconv.Itoa(int(lk.version)))
	// }

	// // try to race for the lock
	// log.Printf("race for the lock")
	// err := lk.ck.Put(lk.l, lk.LockState(), lk.version)

	// // if don't fetch, try at the next turn
	// for err != rpc.OK {
	// 	for state, vers, _ := lk.ck.Get(lk.l); state == lk.UnlockState(); state, vers, _ = lk.ck.Get(lk.l) {
	// 		log.Printf("key: " + lk.l + " state: " + state + " waiting for lock to be released")
	// 		lk.version = vers
	// 		log.Print("version: " + strconv.Itoa(int(lk.version)))
	// 	}

	// 	log.Printf("race for the lock")
	// 	err = lk.ck.Put(lk.l, lk.LockState(), lk.version)
	// }

	// lk.isOwn = true
	// lk.version ++
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
	}

	// here is the code don not use while-retry pattern

	// log.Printf("lock released")

	// // try to release the lock
	// err := lk.ck.Put(lk.l, lk.UnlockState(), lk.version)

	// // if don't fetch, try at the next turn
	// for _, vers, _ := lk.ck.Get(lk.l); err != rpc.OK; _, vers, _ = lk.ck.Get(lk.l) {	
	// 	err = lk.ck.Put(lk.l, lk.UnlockState(), vers)
	// }

	// lk.isOwn = false
	// lk.version++
}
