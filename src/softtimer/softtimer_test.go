package softtimer

import (
    "testing"
    "time"
)

// 等待一个信号，带超时
func waitSignal(ch <-chan int, timeout time.Duration) (int, bool) {
    select {
    case v := <-ch:
        return v, true
    case <-time.After(timeout):
        return 0, false
    }
}

// 断言在一段时间内不应该收到信号
func waitNoSignal(t *testing.T, ch <-chan int, timeout time.Duration) {
    select {
    case v := <-ch:
        t.Fatalf("unexpected signal %v within %v", v, timeout)
    case <-time.After(timeout):
        // ok
    }
}

// 1. 基本自动 tick：Enable 后，间隔到时应该自动发信号
func TestSoftTimer_AutoTick(t *testing.T) {
    interval := 100 * time.Millisecond
    timer := New(interval)
    defer timer.Close()

    timer.Enable(7)

    if v, ok := waitSignal(timer.C, 3*interval); !ok {
        t.Fatalf("expected auto tick within %v, but none", 3*interval)
    } else if v != 7 {
        t.Fatalf("unexpected version: got %v, want 7", v)
    }
}

// 2. Reset 只更新时间，不发 signal（当 enabled）
func TestSoftTimer_ResetNoSignal(t *testing.T) {
    interval := 200 * time.Millisecond
    timer := New(interval)
    defer timer.Close()

    timer.Enable(1)

    // Reset 返回当前版本
    if v := timer.Reset(); v != 1 {
        t.Fatalf("expected Reset to return version 1, got %v", v)
    }

    // 在小于 interval 的时间内，不应该收到任何信号
    waitNoSignal(t, timer.C, interval/2)
}

// 3. Reset 会推迟下一次自动 tick
func TestSoftTimer_ResetDelaysNextTick(t *testing.T) {
    interval := 200 * time.Millisecond
    timer := New(interval)
    defer timer.Close()

    timer.Enable(10)

    // 先等第一次自动 tick
    if v, ok := waitSignal(timer.C, 3*interval); !ok {
        t.Fatalf("expected first auto tick")
    } else if v != 10 {
        t.Fatalf("unexpected version: got %v, want 10", v)
    }

    // 过一点点时间再 Reset，相当于“重置 lastExec”
    time.Sleep(50 * time.Millisecond)
    resetTime := time.Now()

    if v := timer.Reset(); v != 10 {
        t.Fatalf("expected Reset return 10, got %v", v)
    }

    // 接下来 < interval 时间内，不应该再有 tick
    waitNoSignal(t, timer.C, interval-50*time.Millisecond)

    // 再过一段时间，应该出现新的自动 tick
    deadline := interval + 2*interval // 给一点冗余
    if v, ok := waitSignal(timer.C, deadline); !ok {
        t.Fatalf("expected auto tick delayed after Reset() at %v", resetTime)
    } else if v != 10 {
        t.Fatalf("unexpected version after Reset: got %v, want 10", v)
    }
}

// 4. Disable 之后不再自动 tick；Reset 返回 -1 且不更新时间
func TestSoftTimer_DisableStopsAuto(t *testing.T) {
    interval := 100 * time.Millisecond
    timer := New(interval)
    defer timer.Close()

    timer.Enable(3)

    // 等一次自动 tick，确保定时器确实在工作
    if v, ok := waitSignal(timer.C, 3*interval); !ok {
        t.Fatalf("expected first auto tick before disable")
    } else if v != 3 {
        t.Fatalf("unexpected version: got %v, want 3", v)
    }

    timer.Disable()

    // Disable 后不会自动 tick
    waitNoSignal(t, timer.C, 3*interval)

    // Reset 应返回 -1
    if v := timer.Reset(); v != -1 {
        t.Fatalf("expected Reset to return -1 when disabled, got %v", v)
    }

    // Reset 不应发信号
    waitNoSignal(t, timer.C, interval/2)
}

// 5. Disable 后再 Enable，会恢复自动 tick
func TestSoftTimer_EnableAfterDisable(t *testing.T) {
    interval := 100 * time.Millisecond
    timer := New(interval)
    defer timer.Close()

    timer.Enable(5)
    if v, ok := waitSignal(timer.C, 3*interval); !ok {
        t.Fatalf("expected first auto tick")
    } else if v != 5 {
        t.Fatalf("unexpected version: got %v, want 5", v)
    }

    timer.Disable()
    // 确认禁用期间不会自动 tick
    waitNoSignal(t, timer.C, 2*interval)

    // 再 Enable，使用新版 version
    timer.Enable(6)

    if v, ok := waitSignal(timer.C, 3*interval); !ok {
        t.Fatalf("expected auto tick after re-enable")
    } else if v != 6 {
        t.Fatalf("unexpected version after re-enable: got %v, want 6", v)
    }
}

// 6. Close 之后 goroutine 退出，C 被关闭
func TestSoftTimer_Close(t *testing.T) {
    interval := 50 * time.Millisecond
    timer := New(interval)

    timer.Enable(42)
    // 等一次 tick，确保 goroutine 正常跑着
    _, _ = waitSignal(timer.C, 3*interval)

    timer.Close()

    // Close 之后，C 应该被关闭
    _, ok := <-timer.C
    if ok {
        t.Fatalf("expected C to be closed after Close()")
    }
}

// ex. Close() 之后，后续控制调用至少不应该永久阻塞。
// 先前实现里，Close() 会让 loop() 直接退出；
// 此后 Reset()/Enable()/Disable() 往 ctrlCh 发送命令后，
// 会一直等 tempCh 的回复，但已经没有 goroutine 会处理它了，
// 因而这个测试会失败。
func TestSoftTimer_ResetAfterCloseShouldNotBlock(t *testing.T) {
	tm := New(time.Hour)

	// 先关闭，确保 loop() 已经退出
	tm.Close()

	done := make(chan struct{})
	go func() {
		_ = tm.Reset()
		close(done)
	}()

	select {
	case <-done:
		// 理想行为：不要永久阻塞
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Reset() blocked after Close()")
	}
}