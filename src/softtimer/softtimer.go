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
// Use t.Trigger() to manually update lastExec 
// with no signal to C.

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
}

func New(interval time.Duration) *SoftTimer {
	t := &SoftTimer{
		interval: interval,
		C:        make(chan int, 1), // buffer size = 1 makes the most sense
		lastExec: time.Now(),
		enabled:  false, // initially disabled
		closed:   false,
		ctrlCh:   make(chan command),
	}
	t.wg.Add(1)
	go t.loop()
	return t
}

func (t *SoftTimer) Enable(ver int) {
	t.ctrlCh <- command{typ: cmdEnable, ver: ver}
}
func (t *SoftTimer) Disable() {
	t.ctrlCh <- command{typ: cmdDisable}
}
func (t *SoftTimer) Trigger() {
	t.ctrlCh <- command{typ: cmdTrigger}
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
	
	case cmdDisable:
		t.enabled = false

	case cmdTrigger:
		t.update()

	case cmdClose:
		t.closed = true
	}
}

// looping while handling the commands
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