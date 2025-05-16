package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	// "bytes"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm	  		int 			// current term of this Raft peer
	voteFor		  		int				// candidateId that received vote in current term
										// use -1 when haven't voted for anyone
	state	  	  		Status 			// 0 for follower, 1 for candidate, 2 for leader
	commitIndex   		int 			// index of the highest log entry known to be committed
	lastApplied   		int				// index of the last entry applied to the state machine
	logs		  		[]*Entry		// log entries
	heartbeatReceived 	bool			// 0 for no received heartbeat
	leaderId	  		int				// leaderId of the current leader
	nextIndex           []int			// for each server, index of the next log entry to send to that server
	matchIndex          []int			// for each server, index of highest log entry known to be replicated on server
	lastSuccessfulContact []bool 		// each follower's last successful contact with the leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader) // 2 for leader
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


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term 			int // cadidate's term
	CandidateId		int // cadidate requesting vote
	LastLogIndex	int // index of candidate's last log entry
	LastLogTerm		int // term of cadidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term			int  // currentTerm, for candidate to update itself
	VoteGranted		bool // true means cadidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	// if request's term is less than current term, reject
	if args.Term < rf.currentTerm {
		return
	}

	// if request's term is greater than current term, update current term and voteFor, back to Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.voteFor = -1
	}

	// check whether voted and not for this candidate
	if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
		return
	}

	// check whether candidate's log is up-to-date
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term

	// if candidate's log is not up-to-date, quit vote
	if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		return
	}
	
	// if all checks pass, grant vote
	reply.VoteGranted = true
	rf.voteFor = args.CandidateId
	rf.heartbeatReceived = true

	tester.Annotate("Server " + strconv.Itoa(rf.me), strconv.Itoa(rf.me) + " vote for " + strconv.Itoa(args.CandidateId), strconv.Itoa(rf.me) + " vote for " + strconv.Itoa(args.CandidateId))
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term 	 		int 		// the leader's term
	LeaderId 		int 		// so follower can redirect clients
	PrevLogIndex	int 		// index of log entry immediately preceding new ones
	PrevLogTerm		int 		// term of prevLogIndex entry
	Entries         []*Entry	// log entries to store
	LeaderCommit	int			// leader's commitIndex
}

type AppendEntriesReply struct {
	Term			int				// currentTerm, for leader to update itself
	Success			bool			// true if follower contained entry matching prevLogIndex and prevLogTerm
	FalseType       AppendFalseType // false type
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.FalseType = WrongTerm
		return
	}

	// if receive larger term, update current term and voteFor, back to Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.state = Follower
	}

	// reset heartbeat, update leaderId
	rf.heartbeatReceived = true
	rf.leaderId = args.LeaderId
	
	// check consistency of logs
	if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.FalseType = WrongPrevEntry
		return
	}

	// add entries to logs
	reply.Success = true
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(rf.logs) {
			rf.logs[idx] = entry
		} else {
			rf.logs = append(rf.logs, entry)
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = (rf.state == Leader)
	if !isLeader {
		return index, term, isLeader
	}

	index = len(rf.logs)
	term = rf.currentTerm
	entry := &Entry{
		Term: rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs, entry)

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		
		rf.mu.Lock()
		heartbeatReceived := rf.heartbeatReceived
		state := rf.state
		rf.mu.Unlock()

		if state == Leader {
			tester.Annotate("Server " + strconv.Itoa(rf.me), "Server " + strconv.Itoa(rf.me) +" is leader", "Server " + strconv.Itoa(rf.me) +" is leader" + " term :" + strconv.Itoa(rf.currentTerm)) 
		}

		if !heartbeatReceived && state != Leader {
			go rf.election()
		}
		
		rf.mu.Lock()
		tester.Annotate("Server " + strconv.Itoa(rf.me), "Server " + strconv.Itoa(rf.me) +" require heartbeat", "Server " + strconv.Itoa(rf.me) +" require heartbeat")
		rf.heartbeatReceived = false
		rf.mu.Unlock()

		ms := 200 + (rand.Int63() % 400) // 200~600ms
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// when a server becomes leader, it should send heartbeats to all the followers
// to let them know that it is the leader. it should also initialize the nextIndex and matchIndex for each follower.


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
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.heartbeatReceived = false
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.voteFor = -1
	rf.state = Follower
	rf.logs = make([]*Entry, 0)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	atomic.StoreInt32(&rf.dead, 0)
	rf.logs = append(rf.logs, &Entry{
								Term: 0,
								Command: nil,
							})
	rf.lastSuccessfulContact = make([]bool, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
