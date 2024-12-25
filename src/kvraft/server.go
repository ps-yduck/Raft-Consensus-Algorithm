package raftkv

/////         Race Test /////////
//go test "race" gets stuck on sequential testing sometimes in TestUnreliable (because of high number of go routines launched). It works fine on  individual testing. Also there is no data race detected
import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key         string
	Value       string
	ClientId    int64
	OperationId int

	PutVarient string
}

type RaftKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	db           map[string]string
	indexChans   map[int]chan int
	maxraftstate int // snapshot if log grows this big
	currTerm     int
	cmdSeen      map[int64]int

	// Your definitions here.
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	val, ok := kv.cmdSeen[args.ClientId]
	kv.mu.Unlock()
	if ok {
		if val >= args.OperationId {
			reply.Err = Duplicate

			kv.mu.Lock()
			if value, ok := kv.db[args.Key]; ok {
				reply.Value = value
				reply.OperationId = args.OperationId

			} else {
				reply.Value = ""
				reply.OperationId = args.OperationId

			}
			kv.mu.Unlock()
			return

		}
	}

	index, term, isLeader := kv.rf.Start(Op{Key: args.Key, ClientId: args.ClientId, OperationId: args.OperationId})
	if isLeader {
		applyStatus := make(chan int)
		kv.mu.Lock()
		kv.currTerm = term //term when initiate start, helps to find leader change after start
		kv.indexChans[index] = applyStatus
		value, ok := kv.db[args.Key]
		kv.mu.Unlock()
		applied := <-applyStatus
		if applied == 1 {
			reply.WrongLeader = false

			if ok {
				reply.Value = value
				reply.Err = OK
				reply.OperationId = args.OperationId
			} else {
				reply.Value = ""
				reply.Err = OK
				reply.OperationId = args.OperationId
			}

		} else if applied == 0 {
			reply.WrongLeader = true
		}
	} else {
		reply.WrongLeader = true
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	val, ok := kv.cmdSeen[args.ClientId]
	kv.mu.Unlock()
	if ok {
		if val >= args.OperationId {
			reply.Err = Duplicate
			reply.OperationId = args.OperationId

			return
		}
	}

	index, term, isLeader := kv.rf.Start(Op{Key: args.Key, Value: args.Value, ClientId: args.ClientId, OperationId: args.OperationId, PutVarient: args.Op})

	if isLeader {
		applyStatus := make(chan int)
		kv.mu.Lock()
		kv.currTerm = term
		kv.indexChans[index] = applyStatus
		kv.mu.Unlock()
		applied := <-applyStatus
		if applied == 1 {
			reply.WrongLeader = false
			reply.Err = OK
			reply.OperationId = args.OperationId

		} else if applied == 0 {
			reply.WrongLeader = true
		}
	} else {
		reply.WrongLeader = true
	}

}

// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {

	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.db = make(map[string]string)

	kv.indexChans = make(map[int]chan int)
	kv.currTerm = -1
	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.cmdSeen = make(map[int64]int)
	go kv.applyhandler()
	return kv
}

func (kv *RaftKV) applyhandler() {
	for {
		dup := true
		rerequest := false
		appliedMsg := <-kv.applyCh

		appliedOperation := appliedMsg.Command.(Op)
		kv.mu.Lock()

		if val, ok := kv.cmdSeen[appliedOperation.ClientId]; ok {
			if val+1 == appliedOperation.OperationId {

				kv.cmdSeen[appliedOperation.ClientId] = appliedOperation.OperationId
				dup = false
			} else if appliedOperation.OperationId >= val+2 {
				rerequest = true
			}
			//at val equal and less then dup

		} else {
			kv.cmdSeen[appliedOperation.ClientId] = appliedOperation.OperationId
			dup = false
		}

		if !dup {

			if appliedOperation.PutVarient == "Put" {
				kv.db[appliedOperation.Key] = appliedOperation.Value
			} else if appliedOperation.PutVarient == "Append" {
				kv.db[appliedOperation.Key] += appliedOperation.Value

			}
			kv.cmdSeen[appliedOperation.ClientId] = appliedOperation.OperationId

		}

		idx := appliedMsg.Index
		term, _ := kv.rf.GetState()
		idxChan, ok := kv.indexChans[idx]
		store_term := kv.currTerm
		kv.mu.Unlock()

		if store_term != term { //leader changed
			if ok {
				idxChan <- 0
			}
		} else {
			if ok {
				if rerequest {

					idxChan <- 0

				} else {

					idxChan <- 1

				}

			}
		}

	}
}
