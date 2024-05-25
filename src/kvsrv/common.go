package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key           string
	Value         string
	ClientId      int64
	CorrelationId int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key           string
	ClientId      int64
	CorrelationId int64
}

type GetReply struct {
	Value string
}
