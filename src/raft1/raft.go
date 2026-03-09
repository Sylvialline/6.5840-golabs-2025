package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"

	"6.5840/softtimer"
)

const (
	HeartbeatInterval time.Duration = 100 * time.Millisecond + time.Microsecond
	ElectionTimeoutLower time.Duration = 300 * time.Millisecond
	ElectionTimeoutUpper time.Duration = 600 * time.Millisecond
)

func randomElectionTimeout() time.Duration {
	diff := ElectionTimeoutUpper - ElectionTimeoutLower
	return ElectionTimeoutLower + time.Duration(rand.Int63n(int64(diff)))
}

const chanVolume = 100

type RaftState int8 

const (
	Any RaftState = iota
	Follower 
	Candidate
	Leader
)

type (
	indexT int
	termT  int
	idT    int
)

type logEntry struct {
	// 3B
	Command any
	Term    termT
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        idT                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan raftapi.ApplyMsg

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent
	currentTerm termT
	votedFor    idT // initialized to -1 on every new term
	log         []logEntry

	// volatile on all
	commitIndex indexT
	lastApplied indexT // not protected by mu


	// volatile on leaders
	nextIndex  []indexT
	matchIndex []indexT

	// 3A
	// not protected by mu
	n           int // number of peers
	candidateCh chan termT // buffer size = 1 makes the most sense
	timer *softtimer.SoftTimer 

	// protected by mu
	state RaftState
	beats int // count of heartbeats from leader, zeroed each tick

	// 3B
	applyNotify chan indexT
	aeChs       []chan bool // false means killed
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int(rf.currentTerm), rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// RPCs

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
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.toFollower(args.Term, Candidate, true)
	reply.Term = rf.currentTerm
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

type AppendEntriesArgs struct {
	// 3A
	Term termT
	// 3B
	LeaderId idT
	PrevLogIndex indexT
	PrevLogTerm termT
	Entries []logEntry
	LeaderCommit indexT
}

type AppendEntriesReply struct {
	// 3A
	Term termT
	// 3B
	Success bool
}

// AppendEntries RPC handler.
// Sender is a leader.
// Increase beats, check consistency, append entries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.toFollower(args.Term, Leader, true)
	reply.Term = rf.currentTerm
	if lastIndex(rf.log) < args.PrevLogIndex ||
	   rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// do not consistent
		return
	}
	rf.log = rf.log[:args.PrevLogIndex+1] // trunc first: [0, prev]
	rf.log = append(rf.log, args.Entries...)
	reply.Success = true
	N := min(args.LeaderCommit, lastIndex(rf.log))
	if N > rf.commitIndex {
		rf.commitIndex = N
		testSend(rf.applyNotify, N)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (3B).
	rf.mu.Lock()
	if rf.killed() || rf.state != Leader {
		rf.mu.Unlock()
		return -1, -1, false
	}
	index := len(rf.log)
	term := rf.currentTerm
	rf.log = append(rf.log, logEntry{
		Command: command,
		Term: term,
	})
	rf.mu.Unlock()
	
	rf.timer.Reset()
	rf.broadcast(true)
	
	return index, int(term), true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

	// Terminate aeTicker
	rf.timer.Close()
	// Terminate applier
	close(rf.applyNotify)
	// Terminate aeWorkers
	rf.broadcast(false)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = idT(me)

	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh
	rf.bootInit()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.goroutineInit()

	return rf
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
// init methods

func (rf *Raft) bootInit() {
	rf.dead = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.state = Follower
	rf.n = len(rf.peers)
	rf.beats = 0
	rf.votedFor = -1
	rf.candidateCh = make(chan termT, 1)
	rf.timer = softtimer.New(HeartbeatInterval)

	rf.log = append(rf.log, logEntry{}) // might be overwritten by readPersist()
	rf.applyNotify = make(chan indexT, 1)
	rf.aeChs = make([]chan bool, rf.n)

}

func (rf *Raft) goroutineInit() {
	// periodically inform toCandidate() to start elections
	go rf.rvTicker()

	// send heartbeats when it's leader
	go rf.aeTicker()

	// wait for new elections
	go rf.toCandidate()

	go rf.applier()

	for i := 0; i < rf.n; i++ {
		if i == int(rf.me) { continue }
		rf.aeChs[i] = make(chan bool, 4)
		go rf.aeWorker(i)
	}
}


// my methods for 3B

// Goroutine that send an AppendEntries to server
// and deals with the reply.
// When redo is needed, send true to ch.
func (rf *Raft) aeSender(server int, args *AppendEntriesArgs, ch chan bool) {
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok { return }

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.toFollower(reply.Term, Any, true)
	if rf.state != Leader || rf.currentTerm != args.Term { 
		// discard the reply if term changed, even if it's leader again
		// because uncommitted entries can be overwritten by other leaders
		return
	}

	if reply.Success {
		l := len(args.Entries)
		rf.matchIndex[server] = args.PrevLogIndex + indexT(l)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.commit()
	} else {
		// consistency check failed
		rf.nextIndex[server] --
		testSend(ch, true) // try again
	}
}

// Goroutine listening on applyNotify
// Applies newly committed entries to applyCh
// (assume applyCh to be very congested)
// Owns rf.lastApplied (so no lock when use it)
// Return when killed
func (rf *Raft) applier() {
	for i := range(rf.applyNotify) {
		rf.mu.Lock()
		i = rf.commitIndex
		if i <= rf.lastApplied {
			rf.mu.Unlock()
			continue
		}
		log := slices.Clone(rf.log[rf.lastApplied+1 : i+1])
		rf.mu.Unlock()
		for k, e := range log {
			// assume applyCh to be very congested
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command: e.Command,
				CommandIndex: k + int(rf.lastApplied + 1),
			}
		}
		rf.lastApplied = i
	}
}

// called by leader
// calculate new commitIndex
// caller must hold mu
func (rf *Raft) commit() {
	rf.matchIndex[rf.me] = lastIndex(rf.log) // COUNT YOURSELF!
	N := median(rf.matchIndex)
	if N > rf.commitIndex && rf.log[N].Term == rf.currentTerm {
		rf.commitIndex = N
		testSend(rf.applyNotify, N)
	}
}

// AppendEntries RPC manager for `server`.
// Construct the args and limit the amount of AEs
// sending to `server`.
// Call aeSender to send AE and handle its reply.
// Return when killed (on a false signal)
func (rf *Raft) aeWorker(server int) {
	ch := rf.aeChs[server]
	for b := range(ch) {
		if !b { return }
		args := &AppendEntriesArgs{}
		rf.mu.Lock()
		if rf.state != Leader {
			// must not send AEs with new term but as follower
			rf.mu.Unlock()
			continue
		}
		term := rf.currentTerm
		next := rf.nextIndex[server]

		args.Term = term
		args.LeaderId = rf.me
		args.PrevLogIndex = next - 1
		args.PrevLogTerm = rf.log[next - 1].Term
		args.Entries = rf.log[next:]
		args.LeaderCommit = rf.commitIndex
		rf.mu.Unlock()

		go rf.aeSender(server, args, ch)
	}
}

// send true/false to all aeChs
// used to send heartbeats/AEs/kill signals
func (rf *Raft) broadcast(b bool) {
	for i := 0; i < rf.n; i++ {
		if i == int(rf.me) { continue }
		if b {
			testSend(rf.aeChs[i], true)
		} else {
			rf.aeChs[i] <- false
		}
	}
}

// my methods for 3A


// Goroutine listening on rf.timer.C.
// On every signal received from rf.timer.C, 
// send a heartbeat to all peers.
// Guaranteed to return when killed.
func (rf *Raft) aeTicker() {
	for range(rf.timer.C) {
		rf.broadcast(true)
	}
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
		rf.currentTerm = term
		rf.state = Candidate
		rf.votedFor = rf.me
		DPrintf("Server %v becomes candidate in term %v", rf.me, rf.currentTerm)

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
					rf.toFollower(reply.Term, Any, false)
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
	
	rf.nextIndex = make([]indexT, rf.n)
	rf.matchIndex = make([]indexT, rf.n)
	for i := 0; i < rf.n; i++ {
		rf.nextIndex[i] = indexT(len(rf.log))
		rf.matchIndex[i] = 0 // match at index 0
	}

	DPrintf("Server %v becomes leader in term %v", rf.me, rf.currentTerm)
	rf.timer.Enable(int(rf.currentTerm))
}


// locked caller: AppendEntries(), RequestVote()
// unlocked caller: toCandidate(), sendAppendEntries()
// cease to send heartbeat
// argument who indicates who wants me to become follower
// valid transfers: F->F, C->F, L->F
func (rf *Raft) toFollower(term termT, who RaftState, locked bool) {
	if !locked {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	if rf.state == Candidate && who == Leader || rf.state == Follower {
		if rf.currentTerm > term {
			return
		}
	} else {
		if rf.currentTerm >= term {
			return
		}
	}
	// transfer to follower
	rf.state = Follower
	// if who == Leader {
	// 	rf.beats++
	// }
	rf.beats++

	// when C->F, may be the case where rf.currentTerm == term,
	// in which we cannot initialize votedFor
	if rf.currentTerm < term {
		rf.currentTerm = term
		rf.votedFor = -1
	}
	rf.timer.Disable()
	DPrintf("Server %v becomes follower in term %v by %v", rf.me, rf.currentTerm, who.String())
}