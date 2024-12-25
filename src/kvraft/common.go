package raftkv

const (
	OK        = "OK"
	ErrNoKey  = "ErrNoKey"
	Duplicate = "Duplicate"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key         string
	Value       string
	Op          string // "Put" or "Append"
	ClientId    int64
	OperationId int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
	OperationId int
}

type GetArgs struct {
	Key         string
	ClientId    int64
	OperationId int

	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
	OperationId int
}
