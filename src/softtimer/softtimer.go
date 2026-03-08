package softtimer

import (
	"sync"
	"sync/atomic"
	"time"
)

// My soft timer object.
// Send signal to C every interval time.
// Use t.Enable(ver)/t.Disable() to temporarily
// turn on/off the timer,
// where ver is the signal you'll get from C.
// Use t.Close() to close it.
// Use t.Reset() to reset the timer, without any signal sending to C.
// After the first call to Close(), all control functions return immediately.
type SoftTimer struct {
	interval time.Duration
	C chan int

	version int
	lastExec time.Time
	enabled bool
	closed atomic.Int32 // The atomic.Int32 field closed ensures the timer is closed only once.
	done chan struct{}

	ctrlCh chan command
	wg sync.WaitGroup
}

type commandType int8

const (
	cmdEnable commandType = iota
	cmdDisable
	cmdReset
	cmdClose
)

type command struct {
	typ commandType
	ver int
	tempCh chan int
}

func New(interval time.Duration) *SoftTimer {
	t := &SoftTimer{
		interval: interval,
		C:        make(chan int, 1),
		lastExec: time.Now(),
		enabled:  false, // initially disabled
		done:     make(chan struct{}),
		closed:   atomic.Int32{},
		ctrlCh:   make(chan command, 16),
	}
	t.wg.Add(1)
	go t.loop()
	return t
}

func (t *SoftTimer) send(typ commandType,	ver int) int {
	cmd := command{
		typ: typ,
		ver: ver,
		tempCh: make(chan int, 1),
	}

	select{
	case t.ctrlCh <- cmd:
	case <-t.done:
		return -2
	}

	select{
	case v := <-cmd.tempCh:
		return v
	case <-t.done:
		return -2
	}
}

// Enable()/Disable()/Reset()/Close() won't return 
// until loop() processed the command
func (t *SoftTimer) Enable(ver int) {
	t.send(cmdEnable, ver)
}
func (t *SoftTimer) Disable() {
	t.send(cmdDisable, 0)
}

// If enabled, calls to t.Reset() manually update lastExec 
// and return the current version.
// If disabled, t.Reset() returns -1, 
// with no changes in lastExec
func (t *SoftTimer) Reset() int {
	return t.send(cmdReset, 0)
}

func (t *SoftTimer) Close() {
	if(t.closed.CompareAndSwap(0, 1)) {
		close(t.done)
		t.ctrlCh <- command{typ: cmdClose}
		t.wg.Wait()
		close(t.C)
	}
}

func (t *SoftTimer) killed() bool {
	select{
	case <-t.done:
		return true
	default:
		return false
	}
}

func (t *SoftTimer) doCmd(cmd command) {
	switch cmd.typ {
	case cmdEnable:
		t.enabled = true
		t.version = cmd.ver
		cmd.tempCh <- 0
	
	case cmdDisable:
		t.enabled = false
		cmd.tempCh <- 0

	case cmdReset:
		if t.enabled {
			t.update()
			cmd.tempCh <- t.version
		} else {
			cmd.tempCh <- -1
		}

	case cmdClose:
	}
}

// looping while handling the commands
// exclusively owns all vars, so no locks needed
func (t *SoftTimer) loop() {
	defer t.wg.Done()
	for {
		if(t.killed()) { return }
		if !t.enabled {
			// timer disabled
			t.doCmd(<-t.ctrlCh)
			continue
		}
		// timer enabled
		elapsed := time.Since(t.lastExec)
		if elapsed >= t.interval {
			// tick
			t.execute()
			continue
		}

		sleepTime := t.interval - elapsed
		timer := time.NewTimer(sleepTime)
		select {
		case <-timer.C:
			// wake and loop
		case cmd := <-t.ctrlCh:
			t.doCmd(cmd)
		}
		timer.Stop()
	}
}

func (t *SoftTimer) update() {
	t.lastExec = time.Now()
}

func (t *SoftTimer) execute() {
	t.update()
	select{
	case t.C <- t.version:
		// successfully send the tick
	default:
		// discard this tick
	}
	
}