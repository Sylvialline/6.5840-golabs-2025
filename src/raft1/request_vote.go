package raft

import (
	"math/rand"
	"time"
)


const (
	ElectionTimeoutLower time.Duration = 300 * time.Millisecond
	ElectionTimeoutUpper time.Duration = 600 * time.Millisecond
)

func randomElectionTimeout() time.Duration {
	diff := ElectionTimeoutUpper - ElectionTimeoutLower
	return ElectionTimeoutLower + time.Duration(rand.Int63n(int64(diff)))
}

// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// 3A
	Term termT
	CandidateId idT
	// 3B
	LastLogIndex indexT
	LastLogTerm termT
}

// RequestVote RPC reply structure.
type RequestVoteReply struct {
	// 3A
	Term termT
	VoteGranted bool
}

// RequestVote RPC handler.
// Sender is a candidate.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term)
		reply.Term = rf.currentTerm
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}
	// election restriction
	myLastIndex := lastIndex(rf.log)
	myLastTerm := rf.log[myLastIndex].Term
	if args.LastLogTerm < myLastTerm  {
		return
	}
	if args.LastLogTerm == myLastTerm && 
	   args.LastLogIndex < myLastIndex {
		return
	}
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.beats++ // only when grant vote
}

// RequestVote RPC sender. Goroutine.
// Reply is sent to replyCh, and rvSender do not process it.
func (rf *Raft) rvSender(server int, args *RequestVoteArgs, replyCh chan RequestVoteReply) {
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if ok {
		replyCh <- reply
	}
}


func (rf *Raft) rvTicker() {
	for !rf.killed() {
		// Your code here (3A)
		nextTerm := termT(-1)

		rf.mu.Lock()
		if rf.state != Leader && rf.beats == 0 {
			// a leader election should be started.
			nextTerm = rf.currentTerm + 1
		}
		rf.beats = 0
		rf.mu.Unlock()

		if nextTerm != -1 {
			rf.candidateCh <- nextTerm
		}
		// pause for a random amount of time between
		//  `ElectionTimeoutLower` and `ElectionTimeoutUpper`
		time.Sleep(randomElectionTimeout())
	}
	// Terminate goroutine toCandidate()
	close(rf.candidateCh) 
}


// Goroutine listening on candidateCh.
// A `term` comes from candidateCh means the server
// might want to become a candidate of term `term`.
// Return when killed.
// Methods that send to candidateCh: only ticker()
// Valid transfers: F->C or C->C
func (rf *Raft) toCandidate() {
	i, ok := <-rf.candidateCh
	if !ok { return }

	Outer:
	for {
		term := i
		rf.mu.Lock()
		if rf.state == Leader ||    // L->C not allowed
		   rf.currentTerm >= term { // obsolete election
			rf.mu.Unlock()
			i, ok = <-rf.candidateCh
			if !ok { return }
			continue
		}
		// start election
		// transfer to candidate
		DPrintf("S%v in T%v becomes candidate in T%v", rf.me, rf.currentTerm, term)
		rf.currentTerm = term
		rf.state = Candidate
		rf.votedFor = rf.me
		rf.persist()

		// prepare args
		lastLogIndex := lastIndex(rf.log)
		args := RequestVoteArgs{
			Term: term,
			CandidateId: rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm: rf.log[lastLogIndex].Term,
		}
		rf.mu.Unlock()

		// send RVs
		replyCh := make(chan RequestVoteReply, chanVolume)
		for i := 0; i < rf.n; i++ {
			if i == int(rf.me) { continue }
			go rf.rvSender(i, &args, replyCh)
		}

		// count votes
		// while listening on next term from candidateCh
		votes := 1 // COUNT YOURSELF!
		Inner:
		for {
			select {
			case reply := <-replyCh:
				if reply.VoteGranted {
					votes++
					if votes*2 > rf.n {
						break Inner
					}
				} else if reply.Term > term {
					// someone is on a larger term than I'm competing for leader now
					rf.mu.Lock()
					rf.toFollower(reply.Term)
					rf.persist()
					rf.beats++
					rf.mu.Unlock()
					votes = -1
					break Inner
				}

			case i, ok = <-rf.candidateCh:
				if !ok { return }
				// timeout. start another election
				continue Outer
			}
		}
		if votes != -1 {
			// won the election
			rf.toLeader(term)
		}
		// wait for another election
		i, ok = <-rf.candidateCh
		if !ok { return }
	}
}


// called by toCandidate()
// start to send heartbeat
// Including leader initialization
// Valid transfer: C->L
func (rf *Raft) toLeader(term termT) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Candidate || rf.currentTerm != term { return }
	// transfer to leader
	rf.state = Leader
	
	for i := 0; i < rf.n; i++ {
		rf.nextIndex[i] = indexT(len(rf.log))
		rf.matchIndex[i] = 0 // match at index 0
	}

	DPrintf("S%v becomes leader in T%v", rf.me, rf.currentTerm)
	rf.timer.Enable(int(rf.currentTerm))
}