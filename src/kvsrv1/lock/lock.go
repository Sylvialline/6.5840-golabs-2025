package lock

import (
	"log"

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
	name string
	key string
	holding bool
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.name = kvtest.RandValue(8)
	lk.key = l
	lk.holding = false
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	if lk.holding {
		log.Fatalf("%v acquires while holding the lock", lk.name)
	}
	// log.Printf("%v acquire", lk.name)
	for {
		value, version, err := lk.ck.Get(lk.key)
		if err == rpc.ErrNoKey {
			version = 0
			value = ""
		}
		if value == lk.name {
			// consider it a successful acquirement only if value == lk.id
			lk.holding = true
			return
		}
		if value == "" {
			lk.ck.Put(lk.key, lk.name, version)
			// we don't check the return value of Put()
			// because we may also sucess on ErrMaybe
		}
	}
	

}

func (lk *Lock) Release() {
	// Your code here
	if !lk.holding {
		log.Fatalf("%v releases without holding the lock", lk.name)
	}
	value, version, _ := lk.ck.Get(lk.key)
	if value != lk.name {
		log.Fatalf("%v holding the lock while server doesn't think so", lk.name)
	}
	for {
		lk.ck.Put(lk.key, "", version) 
		// we might fail on ErrMaybe
		value, version, _ = lk.ck.Get(lk.key)
		if value != lk.name {
			lk.holding = false
			return
		}
	}
	
}
