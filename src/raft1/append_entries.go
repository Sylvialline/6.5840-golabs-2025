package raft

import "slices"


type AppendEntriesArgs struct {
	// 3A
	Term termT
	// 3B
	LeaderId idT // client redirection (not implemented)
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

// Find the largest offset i <= x such that rf.log[i].term <= k.
// Returns -1 if rf.snapshotIndex is the only i.
// Returns -2 if no such i.
func (rf *Raft) findLE(x offsetT, k termT) offsetT {
	var l   offsetT = -1
	var r   offsetT = min(lastOffset(rf.log), x)
	var res offsetT = -2

	if rf.termAtOffset(r) <= k {
		return r
	}
	for l <= r {
		mid := (l+r) / 2
		if rf.termAtOffset(mid) <= k {
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

	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	
	defer rf.persist()
	
	rf.toFollower(args.Term)
	rf.beats++
	reply.Term = rf.currentTerm
	
	off := rf.toOffset(args.PrevLogIndex)
	if off < -1 {
		// Stage 2:
		// An successful AE reply had been dropped, so nextIndex for 
		// this follower failed to advance as it should do.
		// During this time, follower's service created a new snapshot
		// and advanced follower's snapshotIndex to surpass leader's nextIndex.
		// Approach: truncate the overlapping part of args.Entries 
		truncSize := int(-1 - off)
		if truncSize > len(args.Entries) {
			return
		}
		args.PrevLogIndex += indexT(truncSize)
		args.PrevLogTerm = args.Entries[truncSize-1].Term
		args.Entries = args.Entries[truncSize:]
		off = -1
	}
	if lastOffset(rf.log) < off ||
	   rf.termAtOffset(off) != args.PrevLogTerm {
		// Stage 1: consistency check failed
		x := off
		k := args.PrevLogTerm
		offRes := rf.findLE(x, k)
		// The follower's log at least matches with the leader's
		// at rf.snapshotIndex, in which case offRes gets -1 from findLE() expectedly.
		reply.XIndex = rf.toIndex(offRes)
		reply.XTerm = rf.termAtOffset(offRes)
		return
	}
	

	// Under unreliable networks, an old AppendEntries request may arrive
	// after newer entries have already been accepted.
	// A stale request may roll back entries that should not be removed,
	// potentially including committed ones,
	// which is catastrophic (broken Leadership Completeness)
	// if this follower is to be elected as a new leader afterwards.

	reply.Success = true
	if len(args.Entries) != 0 {
		myLastIndex := rf.lastLogIndex()
		myLastTerm := lastTerm(rf.log)
		newLastIndex := args.PrevLogIndex + indexT(len(args.Entries))
		newLastTerm := lastTerm(args.Entries)
		if newLastTerm < myLastTerm ||
			newLastTerm == myLastTerm && newLastIndex < myLastIndex {
				// Stage 2: Stale request
				// Mimic the election restriction: let only a newer log to overwrite mine.
				return
			}
		rf.log = append(rf.log[:off+1], args.Entries...)
	}

	N := min(args.LeaderCommit, rf.lastLogIndex())
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
			// Stage 2: Stale reply
			// A successful reply may be stale under unreliable networks.
			// Only move replication progress forward: never let an out-of-order
			// old reply roll back matchIndex/nextIndex.
			return
		}
		rf.matchIndex[server] = newMatch
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		rf.commit()
	} else {
		// Stage 1: consistency check failed
		// In this stage, rf.matchIndex[server] == 0 and
		// rf.nextIndex[server] always decrease.
		if rf.matchIndex[server] != 0 {
			// Stage 2:
			// Stale reply: this follower has caught up with leader at least once
			return
		}
		// fast backup
		x := rf.toOffset(reply.XIndex)
		k := reply.XTerm
		newNext := rf.toIndex(rf.findLE(x, k)) + 1
		if rf.nextIndex[server] <= newNext {
			// Stale reply: backup progress is up-to-date
			return
		}
		rf.nextIndex[server] = newNext
		testSend(ch, true) // try again
	}
}


// Heartbeat manager for `server`.
// Goroutine listening on rf.aeChs[server].
// Construct the args and limit
// the amount of heartbeats sending to `server`.
// Call aeSender to send AE,
// or call isSender to send IS
// when rf.nextIndex[server] <= rf.snapshotIndex.
// Return when killed (on a false signal)
func (rf *Raft) hbWorker(server int) {
	ch := rf.aeChs[server]
	for b := range(ch) {
		if !b { return }
		rf.mu.Lock()
		if rf.state != Leader {
			// must not send heartbeats with new term but as follower
			rf.mu.Unlock()
			continue
		}
		term := rf.currentTerm
		next := rf.nextIndex[server]
		if next > rf.snapshotIndex {
			args := &AppendEntriesArgs{
				Term: term,
				LeaderId: rf.me,
				PrevLogIndex: next - 1,
				PrevLogTerm: rf.termAtIndex(next - 1),
				Entries: slices.Clone(rf.log[rf.toOffset(next):]),
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			go rf.aeSender(server, args, ch)
		} else {
			args := &InstallSnapshotArgs{
				Term: term,
				LeaderId: rf.me,
				LastIncludedIndex: rf.snapshotIndex,
				LastIncludedTerm: rf.snapshotTerm,
				Data: slices.Clone(rf.snapshot),
			}
			rf.mu.Unlock()
			go rf.isSender(server, args, ch)
		}
	}
}


// send true/false to all aeChs,
// used to send heartbeats(AEs/ISs)/kill signals
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
func (rf *Raft) hbTicker() {
	for range(rf.timer.C) {
		rf.broadcast(true)
	}
}