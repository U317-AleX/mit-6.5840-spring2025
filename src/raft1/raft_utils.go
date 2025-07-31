package raft

import (
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// leaderInit initializes the Raft instance as a leader and resets the nextIndex and matchIndex fields.
func (rf *Raft) election() {
	rf.mu.Lock()
	if rf.state != Follower && rf.state != Candidate {
		rf.mu.Unlock()
		return
	}
	rf.state = Candidate
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.mu.Unlock()

	votes := int32(1)
	var once sync.Once
	var wg sync.WaitGroup

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(peer int) {
			defer wg.Done()

			rf.mu.Lock()
			args := &RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
				LastLogIndex: len(rf.logs) - 1,
				LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
			}
			rf.mu.Unlock()

			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(peer, args, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.voteFor = -1
				return
			}

			if reply.VoteGranted && rf.state == Candidate {
				newVotes := atomic.AddInt32(&votes, 1)
				if newVotes > int32(len(rf.peers)/2) {
					once.Do(func() {
						go rf.leaderInit()
					})
				}
			}
		}(i)
	}

	wg.Wait()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Candidate && votes > int32(len(rf.peers)/2) {
		once.Do(func() {
			go rf.leaderInit()
		})
	}
}

// leaderInit initializes the Raft instance as a leader and sets up the nextIndex and matchIndex for each peer.
func (rf *Raft) leaderInit() {
	tester.Annotate("Server " + strconv.Itoa(rf.me), "Server " + strconv.Itoa(rf.me) +" comes to leader", "Server " + strconv.Itoa(rf.me) +" comes to leader" + " term " + strconv.Itoa(rf.currentTerm))
	rf.state = Leader
	for i := range rf.peers {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.logs)
	}

	for i := range rf.peers {
		if i != rf.me {
			go rf.entriesSender(i)
		}
	}

	go rf.commitIndexFinder()
	go rf.leaderHealthMonitor()
}

// send entries to a unique follower
func (rf *Raft) entriesSender(peer int) {
	for {
		rf.mu.Lock()

		// if not leader, or killed, return
		if rf.killed() || rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		// if nextIndex is less than or equal to 0, reset it to 1
		if rf.nextIndex[peer] <= 0 {
			rf.nextIndex[peer] = 1 // 防止越界
		}
		prevLogIndex := rf.nextIndex[peer] - 1
		if prevLogIndex >= len(rf.logs) {
			prevLogIndex = len(rf.logs) - 1
		}
		prevLogTerm := rf.logs[prevLogIndex].Term

		entries := make([]*Entry, len(rf.logs[rf.nextIndex[peer]:]))
		copy(entries, rf.logs[rf.nextIndex[peer]:])

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		rf.lastSuccessfulContact[peer] = false
		ok := rf.sendAppendEntries(peer, args, reply)

		if !ok {
			// if send failed, mark the peer as unreachable
			time.Sleep(10 * time.Millisecond)
			continue
		}

		rf.mu.Lock()

		// if term is out of date, step down
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.voteFor = -1
			rf.mu.Unlock()
			return
		}

		if rf.state != Leader || rf.currentTerm != args.Term {
			rf.lastSuccessfulContact[peer] = false
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			// if append success, update nextIndex and matchIndex
			rf.lastSuccessfulContact[peer] = true
			newMatch := args.PrevLogIndex + len(args.Entries)
			rf.matchIndex[peer] = newMatch
			rf.nextIndex[peer] = newMatch + 1
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		} else {
			// else roll back nextIndex
			if reply.FalseType == WrongPrevEntry {
				if rf.nextIndex[peer] > 1 {
					rf.nextIndex[peer]--
				} else {
					rf.nextIndex[peer] = 1
				}
			} else if reply.FalseType == WrongTerm {
				// if term is out of date, step down
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.voteFor = -1
					rf.mu.Unlock()
					return
				}
			}
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// commitIndexFinder finds the commit index for the leader.
func (rf *Raft) commitIndexFinder() {
	for {
		_, isLeader := rf.GetState()
		if !isLeader || rf.killed() {
       		return
    	}

		rf.mu.Lock()
		sorted := make([]int, len(rf.matchIndex))
		copy(sorted, rf.matchIndex)
		sort.Ints(sorted)
		rf.commitIndex = sorted[len(sorted)/2]
		rf.mu.Unlock()
	}
}

// buildAppendEntriesArgs builds the arguments for the AppendEntries RPC.
func (rf *Raft) buildAppendEntriesArgs(peer int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	prevLogIndex := rf.nextIndex[peer] - 1
	prevLogTerm := rf.logs[prevLogIndex].Term
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
	}
	for i := rf.nextIndex[peer]; i < len(rf.logs); i++ {
		args.Entries = append(args.Entries, rf.logs[i])
	}
	return args
}

// leaderHealthMonitor monitors the health of the leader and steps down if it loses quorum.
func (rf *Raft) leaderHealthMonitor() {
	for {
		ms := 600 // 600ms
		tester.Annotate("Server "+strconv.Itoa(rf.me), "LeaderHealthMonitor started", "Leader health monitor loop started")
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		if rf.killed() || rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.lastSuccessfulContact[i] {
				count++
			}
		}

		if count <= len(rf.peers)/2 {
			// quit if lost quorum
			rf.state = Follower
			rf.voteFor = -1
			rf.currentTerm++
			tester.Annotate("Server "+strconv.Itoa(rf.me), "Leader lost quorum and stepped down", "Lost quorum")
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()
	}
}

// committedEntrySender sends committed entries to the apply channel.
func (rf *Raft) committedEntrySender(applyCh chan raftapi.ApplyMsg) {
	for {
		rf.mu.Lock()
		if rf.killed(){
			rf.mu.Unlock()
			return
		}

		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			entry := rf.logs[rf.lastApplied]
			rf.mu.Unlock()
		
			applyCh <- raftapi.ApplyMsg{
				Command:      entry.Command,
				CommandIndex: rf.lastApplied,
				CommandValid: true,
			}

			rf.mu.Lock()
		}

		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}
