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

// 2. Trigger 只更新时间，不发 signal
func TestSoftTimer_TriggerNoSignal(t *testing.T) {
    interval := 200 * time.Millisecond
    timer := New(interval)
    defer timer.Close()

    timer.Enable(1)

    // 立即 Trigger 一下，应该不会立刻有 signal
    timer.Trigger()

    // 在小于 interval 的时间内，不应该收到任何信号
    waitNoSignal(t, timer.C, interval/2)
}

// 3. Trigger 会推迟下一次自动 tick
func TestSoftTimer_TriggerDelaysNextTick(t *testing.T) {
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

    // 过一点点时间再 Trigger，相当于“重置 lastExec”
    time.Sleep(50 * time.Millisecond)
    triggerTime := time.Now()
    timer.Trigger()

    // 接下来 < interval 时间内，不应该再有 tick
    waitNoSignal(t, timer.C, interval-50*time.Millisecond)

    // 再过一段时间，应该出现新的自动 tick
    deadline := interval + 2*interval // 给一点冗余
    if v, ok := waitSignal(timer.C, deadline); !ok {
        t.Fatalf("expected auto tick delayed after Trigger() at %v", triggerTime)
    } else if v != 10 {
        t.Fatalf("unexpected version after Trigger: got %v, want 10", v)
    }
}

// 4. Disable 之后不再自动 tick，但 Trigger 仍然只更新时间，不发 signal
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

    // 关闭后，不应该再有自动 tick
    waitNoSignal(t, timer.C, 3*interval)

    // 调用 Trigger：仍然不应该发 signal（只更新时间）
    timer.Trigger()
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

    timer.Enable(6)
    // 重新 Enable 之后，应该又能自动 tick（版本号应更新）
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
