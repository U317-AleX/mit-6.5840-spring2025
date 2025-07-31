package raft

// define a type for the state of the Raft peer
type Status int

// create a new type for the state of the Raft peer
const (
	Follower Status = iota 	// 0 for follower
	Candidate             	// 1 for candidate
	Leader                	// 2 for leader
)

// define a type for the state of the Raft peer
type AppendFalseType int

// create a new type for the state of the Raft peer
const (
	NetProblem AppendFalseType = iota 	// 0 for network problem
	WrongTerm							// 1 for wrong term
	WrongPrevEntry 						// 2 for wrong previous entry
)