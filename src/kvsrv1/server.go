package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValVers struct {
	Val   string
	Vers  rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex
	
	// Your definitions here.

	// do not use instance as the val, use it's pointer
	kvStore map[string]*ValVers
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.mu = sync.Mutex{}
	kv.kvStore = make(map[string]*ValVers)

	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	
	kv.mu.Lock()
	valVers, ok := kv.kvStore[args.Key]
	
	if ok {
		reply.Value = valVers.Val
		reply.Version = valVers.Vers
		reply.Err = rpc.OK
		kv.mu.Unlock()
		return
	}

	reply.Value = ""
	reply.Version = 0
	reply.Err = rpc.ErrNoKey
	kv.mu.Unlock()
	return
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.

	if args.Version == 0 {
		kv.mu.Lock()
		_, ok := kv.kvStore[args.Key]; 

		if ok {
			reply.Err = rpc.ErrVersion
			kv.mu.Unlock()
			return
		}

		kv.kvStore[args.Key] = &ValVers{
			Val: args.Value,
			Vers: 1,
		}
		
		reply.Err = rpc.OK
		kv.mu.Unlock()
		return
	}

	kv.mu.Lock()
	valVers, ok := kv.kvStore[args.Key]

	if !ok {
		reply.Err = rpc.ErrNoKey
		kv.mu.Unlock()
		return
	}

	if valVers.Vers != args.Version {
		reply.Err = rpc.ErrVersion
		kv.mu.Unlock()
		return
	}

	kv.kvStore[args.Key].Val = args.Value
	kv.kvStore[args.Key].Vers += 1
	reply.Err = rpc.OK

	kv.mu.Unlock()
	return
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
