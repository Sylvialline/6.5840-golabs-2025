package raft

import "slices"


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
// Check consistency (with fast backup), append entries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.toFollower(args.Term)
	rf.beats++
	reply.Term = rf.currentTerm
	if lastIndex(rf.log) < args.PrevLogIndex ||
	   rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// do not consistent
		x := int(args.PrevLogIndex)
		k := args.PrevLogTerm
		reply.XIndex = indexT(findLE(rf.log, x, k))
		reply.XTerm = rf.log[reply.XIndex].Term
		return
	}
	
	// Under unreliable networks, an old AppendEntries request may arrive
	// after newer entries have already been accepted.
	// A stale request may roll back entries that should not be removed,
	// potentially including committed ones,
	// which is catastrophic (broken Leadership Completeness)
	// if this follower is to be elected as a new leader afterwards.
	newLog := rf.log[:args.PrevLogIndex+1]
	newLog = append(newLog, args.Entries...)

	myLastTerm := rf.log[len(rf.log)-1].Term
	newLastTerm := newLog[len(newLog)-1].Term
	if newLastTerm < myLastTerm ||
	   newLastTerm == myLastTerm && len(newLog) < len(rf.log) {
			// Stale reuqest
			// Mimic the election restriction: let only a newer log to overwrite mine.
			return
		}
	rf.log = newLog
	reply.Success = true
	N := min(args.LeaderCommit, lastIndex(rf.log))
	if N > rf.commitIndex {
		rf.commitIndex = N
		testSend(rf.applyNotify, N)
	}
}


// Goroutine that send an AppendEntries to server
// and deals with the reply.
// When redo is needed, send true to ch.
func (rf *Raft) aeSender(server int, args *AppendEntriesArgs, ch chan bool) {
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok { return }

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.toFollower(reply.Term)
		rf.persist()
		rf.beats++
	}
	if rf.state != Leader || rf.currentTerm != args.Term { 
		// discard the reply if term changed, even if it's leader again
		// because uncommitted entries can be overwritten by other leaders
		return
	}

	if reply.Success {
		l := len(args.Entries)
		newMatch := args.PrevLogIndex + indexT(l)
		if (rf.matchIndex[server] >= newMatch) {
			// A successful reply may be stale under unreliable networks.
			// Only move replication progress forward: never let an out-of-order
			// old reply roll back matchIndex/nextIndex.
			return
		}
		rf.matchIndex[server] = newMatch
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.commit()
	} else {
		// consistency check failed
		if rf.matchIndex[server] == lastIndex(rf.log) {
			// Stale reply: this follower is already known to be fully caught up.
			return
		}
		// fast backup
		x := int(reply.XIndex)
		k := reply.XTerm
		newNext := indexT(findLE(rf.log, x, k) + 1)
		if rf.nextIndex[server] <= newNext {
			// Stale reply: backup progress is up-to-date
			return
		}
		rf.nextIndex[server] = newNext
		testSend(ch, true) // try again
	}
}


// AppendEntries RPC manager for `server`.
// Goroutine listening on rf.aeChs[server].
// Construct the args and limit the amount of AEs
// sending to `server`.
// Call aeSender to send AE and let it handle its own reply.
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
		args.Entries = slices.Clone(rf.log[next:])
		args.LeaderCommit = rf.commitIndex
		rf.mu.Unlock()

		go rf.aeSender(server, args, ch)
	}
}


// send true/false to all aeChs,
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


// Goroutine listening on rf.timer.C.
// On every signal received from rf.timer.C, 
// send a heartbeat to all peers.
// Guaranteed to return when killed.
func (rf *Raft) aeTicker() {
	for range(rf.timer.C) {
		rf.broadcast(true)
	}
}