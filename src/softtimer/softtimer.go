package softtimer

import (
	"sync"
	"time"
)

// My soft timer object.
// Send signal to C every interval time.
// Use t.Enable(ver)/t.Disable() to temporarily 
// turn on/off the timer,
// where ver is the signal you'll get from C.
// Use t.Close() to close it.

type SoftTimer struct {
	interval time.Duration
	C chan int

	version int
	lastExec time.Time
	enabled bool
	closed bool

	ctrlCh chan command
	wg sync.WaitGroup
}

type commandType int8

const (
	cmdEnable commandType = iota
	cmdDisable
	cmdTrigger
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
		C:        make(chan int, 1), // buffer size = 1 makes the most sense
		lastExec: time.Now(),
		enabled:  false, // initially disabled
		closed:   false,
		ctrlCh:   make(chan command, 1),
	}
	t.wg.Add(1)
	go t.loop()
	return t
}

// Enable()/Disable()/Trigger()/Close() won't return 
// until loop() processed the command
func (t *SoftTimer) Enable(ver int) {
	ch := make(chan int, 1)
	t.ctrlCh <- command{typ: cmdEnable, ver: ver, tempCh: ch}
	<-ch
}
func (t *SoftTimer) Disable() {
	ch := make(chan int, 1)
	t.ctrlCh <- command{typ: cmdDisable, tempCh: ch}
	<-ch
}
// If enabled, calls to t.Trigger() manually update lastExec 
// and return the current version.
// If disabled, t.Trigger() returns -1, 
// with no changes in lastExec
func (t *SoftTimer) Trigger() int {
	ch := make(chan int, 1)
	t.ctrlCh <- command{typ: cmdTrigger, tempCh: ch}
	return <-ch
}
func (t *SoftTimer) Close() {
	t.ctrlCh <- command{typ: cmdClose}
	t.wg.Wait()
	close(t.C)
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

	case cmdTrigger:
		if t.enabled {
			t.update()
			cmd.tempCh <- t.version
		} else {
			cmd.tempCh <- -1
		}

	case cmdClose:
		t.closed = true
	}
}

// looping while handling the commands
// exclusively owns all vars, so no locks needed
func (t *SoftTimer) loop() {
	defer t.wg.Done()
	for {
		if t.closed { return }
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
		select {
		case <-time.After(sleepTime):
			// wake and loop
		case cmd := <-t.ctrlCh:
			t.doCmd(cmd)
		}

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