package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"cmp"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
	// not protected by mu
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
// Caller must hold mu
func (rf *Raft) persist() {
	// Your code here (3C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		// first boot
		return
	}
	// Your code here (3C).
	var currentTerm termT
	var votedFor idT
	var log []logEntry
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor)    != nil ||
		 d.Decode(&log)         != nil {
		fmt.Printf("readPersist: failed")
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor    = votedFor
	rf.log         = log
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	// ? I don't think mu.Lock() is needed
	// ? What's this method for?
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
	defer rf.persist()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, replyCh chan RequestVoteReply) {
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
	// 3C (Fast Backup)
	XIndex indexT
	XTerm  termT
}

// find the largest index i <= x such that a[i].term <= k
// a[] must be sorted in non-decreasing order
func findLE(a []logEntry, x int, k termT) int {
	l, r, res := 0, min(len(a)-1, x) , -1
	if a[r].Term <= k {
		return r
	}
	for l <= r {
		mid := (l+r) >> 1
		if a[mid].Term <= k {
			res = mid
			l = mid + 1
		} else {
			r = mid - 1
		}
	}
	return res
}

// AppendEntries RPC handler.
// Sender is a leader.
// Increment beats, check consistency, append entries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.beats++
	rf.toFollower(args.Term, Leader, true)
	reply.Term = rf.currentTerm
	if len(rf.log) <= int(args.PrevLogIndex) ||
	   rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// do not consistent
		// x := int(args.PrevLogIndex)
		// k := args.PrevLogTerm
		// reply.XIndex = indexT(findLE(rf.log, x, k))
		// reply.XTerm = rf.log[reply.XIndex].Term
		return
	}
	rf.log = rf.log[:args.PrevLogIndex+1] // trunc first: [0, prev]
	rf.log = append(rf.log, args.Entries...)
	reply.Success = true
	N := min(args.LeaderCommit, lastIndex(rf.log))
	if N > rf.commitIndex {
		rf.commitIndex = N
		select {
		case rf.applyNotify <- N:
		default:
		}
	}
}

// AppendEntries RPC sender.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.toFollower(reply.Term, Any, false)
	}
	return ok
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
	rf.persist()
	rf.mu.Unlock()
	
	rf.timer.Trigger()
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

	// Terminate heartbeatSender
	rf.timer.Close()
	// Terminate applier
	rf.applyNotify <- -1
	// Terminate aeSenders
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

	// From now on, rf.mu needs to be held when use
	// lock protected fields of rf

	rf.goroutineInit()

	return rf
}


func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		rf.mu.Lock()
		if rf.beats == 0 {
			if rf.state == Leader {
				// allow leader to degrade when no one could
				// be connected during a ElectionTimeout
				// MAKE THE TESTER(TestReElection3A) HAPPY
				// (not sure if it's desired)
				rf.state = Follower
			}
			// a leader election should be started.
			select{
			case rf.candidateCh <- (rf.currentTerm + 1):
			default:
			}
		}

		rf.beats = 0
		rf.mu.Unlock()
		// pause for a random amount of time between 500ms and 1000ms
		time.Sleep(randomElectionTimeout())
	}
	// Terminate toCandidate
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
	rf.nextIndex = make([]indexT, rf.n)
	rf.matchIndex = make([]indexT, rf.n)

}

func (rf *Raft) goroutineInit() {
	// periodically inform toCandidate() to start elections
	go rf.ticker()

	// send heartbeats when it's leader
	go rf.heartbeatSender()

	// wait for new elections
	go rf.toCandidate()

	go rf.applier()

	for i := 0; i < rf.n; i++ {
		if i == int(rf.me) { continue }
		rf.aeChs[i] = make(chan bool, chanVolume)
		go rf.aeSender(i)
	}
}


// my methods for 3B

func lastIndex[T any](a []T) indexT {
	return indexT(len(a) - 1)
}

func median[T cmp.Ordered](a []T) T {
	b := slices.Clone(a)
	slices.Sort(b)
	return b[len(b)/2]
}

// Goroutine listening on applyNotify
// Applies newly committed entries to applyCh
// (assume applyCh to be very congested)
// Owns rf.lastApplied (so no lock when use it)
// Return when killed
func (rf *Raft) applier() {
	for i := range(rf.applyNotify) {
		if i == -1 { return }
		rf.mu.Lock()
		i = rf.commitIndex
		rf.mu.Unlock()
		if i <= rf.lastApplied { continue }
		for k := rf.lastApplied + 1; k <= i; k++ {
			rf.mu.Lock()
			e := rf.log[k]
			rf.mu.Unlock()
			// assume applyCh to be very congested
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command: e.Command,
				CommandIndex: int(k),
			}
		}
		rf.lastApplied = i
	}
}

// called by leader.
// calculate new commitIndex.
// caller must hold mu.
func (rf *Raft) commit() {
	rf.matchIndex[rf.me] = lastIndex(rf.log) // COUNT YOURSELF!
	N := median(rf.matchIndex)
	if N > rf.commitIndex && rf.log[N].Term == rf.currentTerm {
		rf.commitIndex = N
		select {
		case rf.applyNotify <- N:
		default:
		}
	}
}

// AppendEntries RPC sender for each server.
// Return when killed (on a false signal).
func (rf *Raft) aeSender(server int) {
	ch := rf.aeChs[server]
	for b := range(ch) {
		if !b { return }
		args := &AppendEntriesArgs{}
		reply := &AppendEntriesReply{}
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
		l := len(args.Entries) // l==0 -> heartbeat
		rf.mu.Unlock()

		ok := rf.sendAppendEntries(server, args, reply)
		if !ok {
			continue
		}

		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != term { 
			// discard the reply if term changed, even if it's leader again
			// because uncommitted entries can be overwritten by other leaders
			rf.mu.Unlock()
			continue
		}

		if reply.Success {
			rf.beats ++
			rf.nextIndex[server] = next + indexT(l)
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			rf.commit()
		} else {
			// consistency check failed
			// fast backup
			// x := int(reply.XIndex)
			// k := reply.XTerm
			// idx := findLE(rf.log, x, k)
			// rf.nextIndex[server] = indexT(idx + 1)
			rf.nextIndex[server] --
			select{
			case ch <- true: // try again
			default:
			}
		}
		rf.mu.Unlock()
	}
}

// send true/false to all aeChs,
// used to send heartbeats/AEs/kill signals
func (rf *Raft) broadcast(b bool) {
	for i := 0; i < rf.n; i++ {
		if i == int(rf.me) { continue }
		rf.aeChs[i] <- b
	}
}

// my methods for 3A


// Goroutine listening on rf.timer.C.
// On every signal received from rf.timer.C, 
// send a heartbeat to all peers.
// Guaranteed to return when killed.
func (rf *Raft) heartbeatSender() {
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
		if rf.state == Leader {
			// L->C not allowed
			rf.mu.Unlock()
			i, ok = <-rf.candidateCh
			if !ok { return }
			continue
		}
		if rf.currentTerm >= term {
			// obsolete election
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
			go rf.sendRequestVote(i, &args, replyCh)
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
			rf.toLeader()
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
func (rf *Raft) toLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Candidate { return }
	// transfer to leader
	rf.state = Leader
	
	for i := 0; i < rf.n; i++ {
		rf.nextIndex[i] = indexT(len(rf.log))
		rf.matchIndex[i] = 0 // match at index 0
	}

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
		defer rf.persist()
		// if locked, perisistence must be done by the caller
		// otherwise, done by toFollower()
	}
	if rf.state == Candidate && who == Leader {
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
	// when C->F, may be the case where rf.currentTerm == term,
	// in which we cannot initialize votedFor
	if rf.currentTerm < term {
		rf.currentTerm = term
		rf.votedFor = -1
	}
	rf.timer.Disable()
}