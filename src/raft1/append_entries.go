package raft

import "slices"

// Log replication for a follower proceeds in three stages once
// snapshots are introduced, since catching up a lagging follower is
// no longer just a matter of sending missing log entries.
//
// The discussion below assumes the leader stays in the same term.
// If the term changes, the new leader reinitializes nextIndex and
// matchIndex for every follower, and the whole process restarts.
//
// Stage 1: fast backup.
// The goal of this stage is to quickly find the highest log index
// that still matches between leader and follower. During this stage,
// matchIndex remains 0, while nextIndex keeps decreasing until the
// first matching index is found. The leader then sends the entries
// after that index and updates matchIndex. Once matchIndex has been
// advanced successfully, the follower will never return to Stage 1
// again within the same leader term, even if the follower restarts.
//
// Stage 2: keeping up.
// After matchIndex has been updated at least once, replication enters
// Stage 2. In this stage, nextIndex is expected to keep pointing to
// the next entry the follower needs, except that it may lag behind
// the follower's actual state when RPC replies are lost. From this
// point on, leader and follower already agree on the prefix, so a
// single AppendEntries RPC is usually enough to synchronize newly
// appended log entries. If log compaction later moves snapshotIndex
// past nextIndex, replication must enter Stage 3.
//
// Stage 3: snapshot installation.
// If nextIndex falls below snapshotIndex in either Stage 1 or Stage 2,
// the leader sends InstallSnapshot directly.
//   - From Stage 1: fast backup has determined that none of the
//     leader's remaining log entries can match the follower.
//   - From Stage 2: communication problems prevent nextIndex from
//     being updated in time, while the service has already created a
//     snapshot beyond it.
// Note that nextIndex (in stage 2) is only the leader's belief about
// the follower's progress; when replies are dropped, it does not
// necessarily point to the follower's actual log end.
//
// A follower accepts an incoming snapshot only if its snapshot index
// is greater than commitIndex, and then advances commitIndex to that
// snapshot index. Here commitIndex serves as a lower bound on the
// follower's replication progress: by Raft's safety properties, all
// log entries up through commitIndex already agree with the leader.
// Therefore, installing a snapshot at or below commitIndex provides
// no new synchronization information and should be rejected as stale.
// This also prevents the follower from accepting a stale
// InstallSnapshot request that survives only because earlier replies
// were dropped.
//
// When a snapshot is accepted, this implementation discards only the
// prefix of rf.log covered by the snapshot and keeps the remaining
// suffix, if any. The remaining suffix may or may not still be valid.
// If Stage 3 was entered from Stage 1, the suffix consists of invalid
// leftover entries skipped during fast backup, and will be removed by
// the next valid AppendEntries. If Stage 3 was entered from Stage 2,
// the suffix may still be valid: dropped replies may have made the
// leader's nextIndex smaller than the follower's actual progress.
// Such a suffix must not be cleared blindly, otherwise replication
// progress may be rolled back, and rf.lastApplied or rf.commitIndex
// may end up pointing to missing log entries.
//
// Progress direction in log replication.
// The leader uses nextIndex and matchIndex together to represent a
// follower's replication progress. Since progress must never go
// backward, its meaning should be stated explicitly in each stage.
//
// In Stage 1, the leader has not yet synchronized successfully with
// the follower, so matchIndex remains 0. Here nextIndex means that
// follower entries with indexes greater than or equal to nextIndex
// have been ruled out as matching the leader log, so progress is made
// by decreasing nextIndex.
//
// In Stage 2 and Stage 3, matchIndex = nextIndex - 1 always holds, and
// both values advance monotonically as replication progresses.

type AppendEntriesArgs struct {
	Term termT
	LeaderId idT // client redirection (not implemented)
	PrevLogIndex indexT
	PrevLogTerm termT
	Entries []logEntry
	LeaderCommit indexT
}

type AppendEntriesReply struct {
	Term termT
	Success bool
	XIndex indexT
	XTerm  termT
	// Progress Feedback: follower reports its current replication
	// progress via LastIndex, so the leader can update nextIndex/
	// matchIndex based on the follower's actual state rather than
	// only the expected effect of this RPC.
	// This field is valid only when Success == true (in log replication stage 2).
	LastIndex indexT
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
		// Stage 2: stale request caused by dropped reply
		// An successful AE reply had been dropped, so nextIndex for 
		// this follower failed to advance as it should do.
		// During this time, follower's service created a new snapshot
		// and advanced follower's snapshotIndex to surpass leader's nextIndex.
		// Approach: truncate the overlapping part of args.Entries 
		truncSize := int(-1 - off)
		if truncSize > len(args.Entries) {
			reply.Success = true
			reply.LastIndex = rf.lastLogIndex()
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
	myLastIndex := rf.lastLogIndex()
	myLastTerm := rf.termAtIndex(myLastIndex)
	var newLastIndex indexT
	var newLastTerm termT
	if len(args.Entries) != 0 {
		newLastIndex = args.PrevLogIndex + indexT(len(args.Entries))
		newLastTerm = lastTerm(args.Entries)
	} else {
		newLastIndex = args.PrevLogIndex
		newLastTerm = args.PrevLogTerm
	}
	
	// A request that would make the log look older is considered stale only
	// after the follower has obtained a log entry in the leader's current term.
	// Otherwise, this may just be the first legitimate overwrite that truncates
	// divergent suffix entries.
	if myLastTerm == args.Term && 
		(newLastTerm < myLastTerm ||
		newLastTerm == myLastTerm && newLastIndex < myLastIndex) {
		// Stage 2: stale request
		// Mimic the election restriction: let only a newer log to overwrite mine.
		reply.LastIndex = rf.lastLogIndex()
		return
	}
	rf.log = append(rf.log[:off+1], args.Entries...)
	reply.LastIndex = rf.lastLogIndex()

	N := min(args.LeaderCommit, reply.LastIndex)
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
		newMatch := reply.LastIndex
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
			// Stale reply: my backup progress is newer
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