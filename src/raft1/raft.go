package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
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
	ElectionTimeoutLower time.Duration = 500 * time.Millisecond
	ElectionTimeoutUpper time.Duration = 1000 * time.Millisecond
)

func randomElectionTimeout() time.Duration {
	diff := ElectionTimeoutUpper - ElectionTimeoutLower
	return ElectionTimeoutLower + time.Duration(rand.Int63n(int64(diff)))
}

const chanVolume = 100

type RaftState int8 

const (
	Follower RaftState = iota
	Candidate
	Leader
)

type (
	indexT int
	termT  int
	idT    int
)

type logEntry struct {
	command any
	term    termT
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        idT                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent
	// protected by mu
	currentTerm termT
	votedFor    idT // initialized to -1 on every new term
	log         []logEntry // 1-indexed

	// volatile on all
	commitIndex indexT
	lastApplied indexT

	// volatile on leaders
	nextIndex  []indexT
	matchIndex []indexT

	// my defined fields (3A)
	
	// not protected by mu
	n           int // number of peers
	candidateCh chan termT // buffer size = 1 makes the most sense

	// protected by mu
	state RaftState
	beats int // count of heartbeats from leader, zeroed each tick
	timer *softtimer.SoftTimer    
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (3A).
	term = int(rf.currentTerm)
	isleader = rf.state == Leader

	return term, isleader
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


// RPCs (3A)

// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term termT
	CandidateId idT
	// LastLogIndex indexT
	// LastLogTerm termT
}

// RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here (3A).
	Term termT
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term, true)
	}
	reply.Term = rf.currentTerm
	if rf.votedFor == -1 {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}
}

// RequestVote RPC sender.
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
}

type AppendEntriesReply struct {
	// 3A
	Term termT
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term, true)
	}
	reply.Term = rf.currentTerm
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok && reply.Term > args.Term {
		rf.toFollower(reply.Term, false)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).


	return index, term, isLeader
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

	// this line will close timer.C, hence end the loop in heartbeatSender()
	rf.timer.Close()
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
	rf.bootInit()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// periodically inform toCandidate() to start elections
	go rf.ticker()

	// send heartbeats when it's leader
	go rf.heartbeatSender()

	// wait for new elections
	go rf.toCandidate()

	return rf
}


func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		rf.mu.Lock()
		if rf.state != Leader && rf.beats == 0 {
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

	close(rf.candidateCh) // stop toCandidate()
}

// my methods for 3A

func (rf *Raft) sendHeartbeat(server int, term termT) {
	args := &AppendEntriesArgs{}
	reply := &AppendEntriesReply{}
	args.Term = term
	rf.sendAppendEntries(server, args, reply)
}

// Goroutine listening on rf.timer.C.
// On every signal received from rf.timer.C, 
// send a heartbeat to all peers.
// Guaranteed to return when killed.
func (rf *Raft) heartbeatSender() {
	for term := range(rf.timer.C) {
		for i := 0; i < rf.n; i++ {
			if i == int(rf.me) { continue }
			go rf.sendHeartbeat(i, termT(term))
		}
	}
}

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
			continue
		}
		if rf.currentTerm >= term {
			// obsolete election
			rf.mu.Unlock()
			continue
		}
		// start election
		// transfer to candidate
		rf.currentTerm = term
		rf.state = Candidate
		rf.votedFor = rf.me

		// prepare args
		// lastLogIndex := indexT(len(rf.log)-1)
		args := RequestVoteArgs{
			Term: term,
			CandidateId: rf.me,
			// LastLogIndex: lastLogIndex,
			// LastLogTerm: termT(rf.log[lastLogIndex].term),
		}
		rf.mu.Unlock()

		// send RVs
		replyCh := make(chan RequestVoteReply, chanVolume)
		for i := 0; i < rf.n; i++ {
			if i == int(rf.me) { continue }
			rf.sendRequestVote(i, &args, replyCh)
		}

		// count votes
		// while listening on next term from candidateCh
		votes := 0
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
					rf.toFollower(reply.Term, false)
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
// Valid transfer: C->L
func (rf *Raft) toLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Candidate { return }
	// transfer to leader
	rf.state = Leader
	
	rf.nextIndex = make([]indexT, rf.n)
	rf.matchIndex = make([]indexT, rf.n)
	for i := 0; i < rf.n; i++ {
		rf.nextIndex[i] = indexT(len(rf.log))
		rf.matchIndex[i] = 0
	}

	rf.timer.Enable(int(rf.currentTerm))
}


// called by: toCandidate(), sendAppendEntries()
// cease to send heartbeat
// valid transfers: F->F, C->F, L->F
func (rf *Raft) toFollower(term termT, locked bool) {
	if !locked {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	if rf.state == Follower && rf.currentTerm >= term ||
		 rf.state == Candidate && rf.currentTerm > term ||
		 rf.state == Leader && rf.currentTerm >= term {
		// obsolete request
		return
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