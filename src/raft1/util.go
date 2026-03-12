package raft

import (
	"strings"
	"cmp"
	"fmt"
	"log"
	"slices"
)

// Debugging
const Debug = false

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

func (rf *Raft) String() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// build log summary
	var logStr strings.Builder
	for i := range rf.log {
		logStr.WriteString(fmt.Sprintf("(%d,%v)", rf.log[i].Term, rf.log[i].Command))
		if i != len(rf.log)-1 {
			logStr.WriteString(" ")
		}
	}

	s := fmt.Sprintf(
		"\nServer %d:\n T%d %v | commit=%d applied=%d | log=[%s]",
		rf.me,
		rf.currentTerm,
		rf.state.String(),
		rf.commitIndex,
		rf.lastApplied,
		logStr.String(),
	)

	if rf.state == Leader {
		s += fmt.Sprintf(
			"\n | next=%v match=%v",
			rf.nextIndex,
			rf.matchIndex,
		)
	}

	return s
}