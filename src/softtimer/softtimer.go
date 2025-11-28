package softtimer

import (
	"sync"
	"time"
)

// My soft timer object.
// Send signal to C every interval time.
// Use t.Enable()/t.Disable() to temporarily 
// turn on/off the timer.
// Use t.Close() to close it.
// Use t.Trigger() to manually update lastExec 
// with no signal to C.

type SoftTimer struct {
	interval time.Duration
	C chan struct{}

	lastExec time.Time
	enabled bool
	closed bool

	ctrlCh chan command
	wg sync.WaitGroup
}

type command int8

const (
	cmdEnable command = iota
	cmdDisable
	cmdTrigger
	cmdClose
)

func New(interval time.Duration) *SoftTimer {
	t := &SoftTimer{
		interval: interval,
		C:        make(chan struct{}),
		lastExec: time.Now(),
		enabled:  false,
		closed:   false,
		ctrlCh:   make(chan command),
	}
	t.wg.Add(1)
	go t.loop()
	return t
}

func (t *SoftTimer) Enable() {
	t.ctrlCh <- cmdEnable
}
func (t *SoftTimer) Disable() {
	t.ctrlCh <- cmdDisable
}
func (t *SoftTimer) Trigger() {
	t.ctrlCh <- cmdTrigger
}
func (t *SoftTimer) Close() {
	t.ctrlCh <- cmdClose
	t.wg.Wait()
	close(t.C)
	close(t.ctrlCh)
}

func (t *SoftTimer) doCmd(cmd command) {
	switch cmd {
	case cmdEnable:
		t.enabled = true
	
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
	case t.C <- struct{}{}:
		// successfully send the tick
	default:
		// discard this tick
	}
	
}