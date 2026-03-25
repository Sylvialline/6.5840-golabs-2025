package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"

	"6.5840/softtimer"
)

const (
	HeartbeatInterval time.Duration = 100 * time.Millisecond + time.Microsecond
)

const chanVolume = 100

type RaftState int8 

const (
	Any RaftState = iota
	Follower 
	Candidate
	Leader
)

type (
	// indexT is the logical index of an entry in the Raft log, independent of log compaction.
	indexT  int
	// offsetT is a zero-based offset into rf.log, relative to the current in-memory log slice.
	offsetT int

	termT int
	idT   int
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
	lastApplied indexT

	// volatile on leaders
	nextIndex  []indexT
	matchIndex []indexT

	// 3A & 3B
	// not protected by mu
	n           int // number of peers
	candidateCh chan termT // buffer size = 1 makes the most sense
	timer *softtimer.SoftTimer 
	applyNotify chan indexT
	aeChs       []chan bool // false means killed

	// protected by mu
	state RaftState
	beats int // count of heartbeats from leader, zeroed each tick

	// 3D: Snapshot
	// persistent and protected by mu
	snapshot      []byte
	snapshotIndex indexT
	snapshotTerm  termT

// rf.applyMu serializes deliveries to applyCh and **exclusively** protects lastApplied.
//
// It is separated from the general rf.mu to avoid a potential deadlock:
// the service layer, while handling messages from applyCh, may call rf.Snapshot(),
// which attempts to acquire rf.mu. Meanwhile, the applier goroutine may hold rf.mu
// and try to send on applyCh, creating a circular wait.
//
// By introducing applyMu, we decouple applyCh delivery from rf.mu,
// ensuring that sending ApplyMsg does not block while holding rf.mu.
	applyMu       sync.Mutex
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
// Caller must hold mu
func (rf *Raft) persist() {
	// Your code here (3C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	if len(data) < 1 { // bootstrap without any state?
		// first boot
		return
	}
	// Your code here (3C).
	var currentTerm termT
	var votedFor idT
	var log []logEntry
	var snapshotIndex indexT
	var snapshotTerm termT
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&currentTerm)   != nil ||
	   d.Decode(&votedFor)      != nil ||
		 d.Decode(&log)           != nil ||
		 d.Decode(&snapshotIndex) != nil ||
		 d.Decode(&snapshotTerm)  != nil {
		panic("readPersist: decode error")
	}
	rf.currentTerm = currentTerm
	rf.votedFor    = votedFor
	rf.log         = log
	rf.snapshotIndex = snapshotIndex
	rf.snapshotTerm  = snapshotTerm

	if rf.snapshotIndex != -1 {
		rf.snapshot = snapshot
		rf.commitIndex = rf.snapshotIndex
		rf.lastApplied = rf.snapshotIndex
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
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
	index := rf.nextLogIndex()
	term := rf.currentTerm
	rf.log = append(rf.log, logEntry{
		Command: command,
		Term: term,
	})
	rf.persist()
	rf.mu.Unlock()
	
	rf.timer.Reset()
	rf.broadcast(true)
	
	return int(index), int(term), true
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
	rf.applyNotify <- -1
	// Terminate aeWorkers
	rf.broadcast(false)

	// DPrintf(rf.String())
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
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// From now on, rf.mu needs to be held when use
	// lock protected fields of rf

	rf.goroutineInit()

	return rf
}

// init methods

func (rf *Raft) bootInit() {
	rf.dead = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.snapshotIndex = -1
	rf.snapshotTerm = -1
	rf.snapshot = nil

	rf.state = Follower
	rf.n = len(rf.peers)
	rf.beats = 0
	rf.votedFor = -1
	rf.candidateCh = make(chan termT, 1)
	rf.timer = softtimer.New(HeartbeatInterval)

	rf.log = append(rf.log, logEntry{}) // might be overwritten by readPersist()
	rf.applyNotify = make(chan indexT, 1)
	rf.aeChs = make([]chan bool, rf.n)
	rf.nextIndex = make([]indexT, rf.n)
	rf.matchIndex = make([]indexT, rf.n)

}

func (rf *Raft) goroutineInit() {
	// periodically inform toCandidate() to start elections
	go rf.rvTicker()

	// send heartbeats when it's leader
	go rf.hbTicker()

	// wait for new elections
	go rf.toCandidate()

	go rf.applier()

	for i := 0; i < rf.n; i++ {
		if i == int(rf.me) { continue }
		rf.aeChs[i] = make(chan bool, 4)
		go rf.hbWorker(i)
	}
}


// Goroutine listening on applyNotify
// Applies newly committed entries to applyCh
// Return when killed
func (rf *Raft) applier() {
	for i := range(rf.applyNotify) {
		if i == -1 { return }
		rf.mu.Lock()
		rf.applyMu.Lock()
		i = rf.commitIndex
		j := rf.lastApplied
		if j >= i {
			// rf.lastApplied > rf.commitIndex may not be possible
			rf.applyMu.Unlock()
			rf.mu.Unlock()
			continue
		}
		log := slices.Clone(rf.log[rf.toOffset(j+1) : rf.toOffset(i+1)])
		rf.mu.Unlock()
		for k, e := range log {
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command: e.Command,
				CommandIndex: k + int(j + 1),
			}
		}
		rf.lastApplied = i
		rf.applyMu.Unlock()
	}
}

// called by leader.
// calculate new commitIndex.
// caller must hold mu.
func (rf *Raft) commit() {
	rf.matchIndex[rf.me] = rf.lastLogIndex() // COUNT YOURSELF!
	N := median(rf.matchIndex)
	if N > rf.commitIndex && rf.termAtIndex(N) == rf.currentTerm {
		rf.commitIndex = N
		testSend(rf.applyNotify, N)
	}
}

// Callers:
// AppendEntries(), RequestVote(), InstallSnapshot(),
// toCandidate(), aeSender(), isSender().
// Cease to send heartbeat.
// Caller must hold mu.
func (rf *Raft) toFollower(term termT) {

	rf.state = Follower

	// when C->F, may be the case where rf.currentTerm == term,
	// in which we cannot initialize votedFor
	if rf.currentTerm < term {
		rf.currentTerm = term
		rf.votedFor = -1
	}
	rf.timer.Disable()
	
}