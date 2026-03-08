package raft

import (
	"cmp"
	"log"
	"slices"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func lastIndex[T any](a []T) indexT {
	return indexT(len(a) - 1)
}

func median[T cmp.Ordered](a []T) T {
	b := slices.Clone(a)
	slices.Sort(b)
	return b[len(b)/2]
}

func testSend[T any](ch chan T, s T) bool {
	if(len(ch) != 0) { return false }
	select{
	case ch <- s:
		return true
	default:
		return false
	}
}

func (s RaftState) String() string {
	return [...]string{
		"any",
		"follower",
		"candidate",
		"leader",
	}[s]
}