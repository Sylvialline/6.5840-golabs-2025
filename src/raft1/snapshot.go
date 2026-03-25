package raft

import (
	"slices"

	"6.5840/raftapi"
)

// Helpers for working with logical log indexes and offsets.
// Caller must hold rf.mu.

// Converts an index of the logical log to its offset in rf.log.
func (rf *Raft) toOffset(index indexT) offsetT {
	return offsetT(index - rf.snapshotIndex - 1)
}

// Converts an offset in rf.log to the corresponding log index.
func (rf *Raft) toIndex(offset offsetT) indexT {
	return indexT(offset) + rf.snapshotIndex + 1
}

func (rf *Raft) termAtOffset(offset offsetT) termT {
	if offset < -1 {
		return -1
	}
	if offset == -1 {
		return rf.snapshotTerm
	}
	return rf.log[offset].Term
}

// Returns the term at logical log index i.
// Returns -1 if i is older than the snapshot boundary.
func (rf *Raft) termAtIndex(i indexT) termT {
	return rf.termAtOffset(rf.toOffset(i))
}

func (rf *Raft) lastLogIndex() indexT {
	return rf.toIndex(lastOffset(rf.log))
}

func (rf *Raft) nextLogIndex() indexT {
	return rf.toIndex(nextOffset(rf.log))
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	i := indexT(index)
	if i <= rf.snapshotIndex { return }
	
	rf.snapshotTerm = rf.termAtIndex(i)
	rf.log = rf.log[rf.toOffset(i+1) :]
	rf.snapshotIndex = i
	rf.snapshot = snapshot
	rf.persist()
}


// InstallSnapshot RPC arguments structure.
type InstallSnapshotArgs struct {
	Term termT
	LeaderId idT // client redirection (not implemented)
	LastIncludedIndex indexT
	LastIncludedTerm termT
	Data []byte
}

// InstallSnapshot RPC reply structure.
type InstallSnapshotReply struct {
	Term termT
}

// InstallSnapshot RPC handler.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	
	rf.toFollower(args.Term)
	rf.beats++
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.snapshotIndex {
		// Stale request: snapshot through this index already installed
		// Also, this case may be caused by a dropped IS reply, in which case
		// the leader should catch up the progress lost by that reply (update nextIndex).
		rf.persist()
		rf.mu.Unlock()
		return
	}

	truncSize := int(args.LastIncludedIndex - rf.snapshotIndex)
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	if truncSize > len(rf.log) {
		rf.log = nil
	} else {
		rf.log = rf.log[truncSize:]
	}
	// rf.log = nil
	rf.snapshot = args.Data
	rf.commitIndex = rf.snapshotIndex
	msg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot: slices.Clone(rf.snapshot),
		SnapshotIndex: int(rf.snapshotIndex),
		SnapshotTerm: int(rf.snapshotTerm),
	}
	index := rf.snapshotIndex
	rf.persist()
	rf.mu.Unlock()

	rf.applyMu.Lock()
	defer rf.applyMu.Unlock()
	if rf.lastApplied >= index {
		return
	}
	rf.applyCh <- msg
	rf.lastApplied = index
}

// Goroutine that send an InstallSnapshot to server
// and deals with the reply.
// Send true to ch to replicate rf.log on the follower via AppendEntries
func (rf *Raft) isSender(server int, args *InstallSnapshotArgs, ch chan bool) {
	reply := InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, &reply)
	if !ok { return }

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.toFollower(reply.Term)
		rf.persist()
		rf.beats++
	}
	if rf.state != Leader || rf.currentTerm != args.Term { 
		return
	}
	
	newMatch := args.LastIncludedIndex
	if rf.matchIndex[server] >= newMatch {
		// Stale reply
		return
	}
	rf.matchIndex[server] = newMatch
	rf.nextIndex[server] = rf.matchIndex[server] + 1
	rf.commit()
	// since nextIndex is now valid, leader can send the rest of its log using AppendEntries
	testSend(ch, true)
}