package raft

type Entry struct {
	Term		int
	Command		interface{}
}
