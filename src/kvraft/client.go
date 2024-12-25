package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"sync"
)

type Clerk struct {
	servers     []*labrpc.ClientEnd
	clientId    int64
	operationId int
	leaderId    int
	mu          sync.Mutex
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.operationId = 0
	ck.leaderId = 0

	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	args := GetArgs{Key: key, ClientId: ck.clientId, OperationId: ck.operationId}

	for {
		reply := new(GetReply)

		ok := ck.servers[ck.leaderId].Call("RaftKV.Get", &args, reply)

		if ok && reply.WrongLeader == false && reply.Err == OK { //set reply.err empty  in server when err in put

			ck.operationId = reply.OperationId + 1

			return reply.Value
		} else if reply.Err == Duplicate {

			ck.operationId = reply.OperationId + 1

			return reply.Value
		} else {

			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)

		}
	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, OperationId: ck.operationId}

	for {
		reply := new(PutAppendReply)

		ok := ck.servers[ck.leaderId].Call("RaftKV.PutAppend", &args, reply)

		if ok && reply.WrongLeader == false && reply.Err == OK { //set reply.err empty  in server when err in put
			ck.operationId = reply.OperationId + 1
			break
		} else if reply.Err == Duplicate {
			ck.operationId = reply.OperationId + 1
			break
		} else {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
