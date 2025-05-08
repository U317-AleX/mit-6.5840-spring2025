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
	state	  	  		int 			// 0 for follower, 1 for candidate, 2 for leader
	commitIndex   		int 			// index of the highest log entry applied to state machine
	lastApplied   		int				// index of the last entry applied to the state machine
	logs		  		map[int]*Entry	// log entries
	heartbeatReceived 	bool			// 0 for no received heartbeat
}

type Entry struct {
	term		int
	command		interface{}
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
	isleader = (rf.state == 2)
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
	// LastLogIndex	int // index of candidate's last log entry
	// LastLogTerm		int // term of cadidate's last log entry
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
	// log.Printf("candidate: " + strconv.Itoa(rf.me) + " begin to handle voteRequest from: " + strconv.Itoa(args.CandidateId))

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = 0
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		rf.heartbeatReceived = true
		tester.Annotate("server" + strconv.Itoa(rf.me),"server" + strconv.Itoa(rf.me)+" reply voteRequest "+"term"+strconv.Itoa(args.Term)," server term "+strconv.Itoa(term)+" args term "+strconv.Itoa(args.Term)+" from:" + strconv.Itoa(args.CandidateId)+"reply:" + strconv.FormatBool(reply.VoteGranted))
		return
	}
	
	reply.VoteGranted = false
	tester.Annotate("server" + strconv.Itoa(rf.me),"server" + strconv.Itoa(rf.me)+" reply voteRequest "+"term"+strconv.Itoa(args.Term)," server term "+strconv.Itoa(term)+" args term "+strconv.Itoa(args.Term)+" from:" + strconv.Itoa(args.CandidateId)+"reply:" + strconv.FormatBool(reply.VoteGranted))
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
	// PrevLogIndex	int 		// index of log entry immediately preceding new ones
	// Entries         []*Entry	// log entries to store
	// LeaderCommit	int			// leader's commitIndex
}

type AppendEntriesReply struct {
	Term			int			// currentTerm, for leader to update itself
	Success			bool		// true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		tester.Annotate("server"+strconv.Itoa(rf.me),"server" + strconv.Itoa(rf.me)+" reply appendRequest "+"term"+strconv.Itoa(args.Term)," server term "+strconv.Itoa(term)+" args term "+strconv.Itoa(args.Term)+" from:" + strconv.Itoa(args.LeaderId)+"reply:" + strconv.FormatBool(reply.Success))
		return
	}
	reply.Success = true

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}

	rf.heartbeatReceived = true

	if rf.state != 0 {
		rf.state = 0
	}

	tester.Annotate("server"+strconv.Itoa(rf.me),"server" + strconv.Itoa(rf.me)+" reply appendRequest "+"term"+strconv.Itoa(args.Term)," server term "+strconv.Itoa(term)+" args term "+strconv.Itoa(args.Term)+" from:" + strconv.Itoa(args.LeaderId)+"reply:" + strconv.FormatBool(reply.Success))
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
		
		// if heartbeat haven't been received, start election
		rf.mu.Lock()
		heartbeatReceived := rf.heartbeatReceived
		rf.mu.Unlock()

		// if there is no heartbeat, entry election
		if !heartbeatReceived {
			go rf.election()
		}
		
		rf.mu.Lock()
		rf.heartbeatReceived = false
		rf.mu.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300) // 50~350ms
		// ms := 150 + rand.Intn(150)  // 150~300ms
		// ms := 150 + (rand.Int63() % 350)  // 150~500ms
		ms := 200 + (rand.Int63() % 400) // 200~600ms
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) election() {
	var wg sync.WaitGroup
	cnt := 1
	rf.mu.Lock()
	rf.state = 1
	rf.currentTerm ++
	rf.voteFor = rf.me
	tester.Annotate("server" + strconv.Itoa(rf.me),"server" + strconv.Itoa(rf.me)+" new a election "+"term:"+strconv.Itoa(rf.currentTerm),"candidate:"+strconv.Itoa(rf.me))

	args := &RequestVoteArgs{
			Term: rf.currentTerm,
			CandidateId: rf.me,
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		rf.mu.Lock()
		flag := rf.state == 1
		rf.mu.Unlock()
		if flag && i != rf.me {
			wg.Add(1)
			go func (peer int)  {
				defer wg.Done()
				reply := &RequestVoteReply{}
				rf.sendRequestVote(peer, args, reply)
	
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = 0
					rf.voteFor = -1
					return
				}
				if reply.VoteGranted {
					cnt++
				}
				if rf.state == 1 && cnt > len(rf.peers)/2 {
					rf.state = 2
					go rf.heart()
					tester.Annotate("server" + strconv.Itoa(rf.me),"server" + strconv.Itoa(rf.me)+" become new leader term "+strconv.Itoa(rf.currentTerm),"become new leader term "+strconv.Itoa(rf.currentTerm))
				}
			}(i)
		}
	}

	wg.Wait()
	rf.mu.Lock()
	if rf.state == 1 && cnt > len(rf.peers)/2 {
		rf.state = 2
		go rf.heart()
		tester.Annotate("server" + strconv.Itoa(rf.me),"server" + strconv.Itoa(rf.me)+" become new leader "+"term"+strconv.Itoa(rf.currentTerm), "become new leader"+"term"+strconv.Itoa(rf.currentTerm))
	}
	rf.mu.Unlock()
}

func (rf *Raft) heart() {
	for _, isLeader := rf.GetState(); isLeader; _, isLeader = rf.GetState() {
		rf.mu.Lock()
		rf.heartbeatReceived = true
		args := &AppendEntriesArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
		}
		rf.mu.Unlock()
		for i := range rf.peers {
			if i != rf.me {
				go func ()  {
					reply := &AppendEntriesReply{}
					rf.sendAppendEntries(i, args, reply)
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = 0
						rf.voteFor = -1
						return
					}
				}()
			}
		}

		ms := 10
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
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
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.heartbeatReceived = false
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.logs = make(map[int]*Entry)
	rf.voteFor = -1
	rf.state = 0
	atomic.StoreInt32(&rf.dead, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
