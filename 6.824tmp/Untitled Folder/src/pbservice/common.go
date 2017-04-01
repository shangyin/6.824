package pbservice

import (
	"fmt"
	"viewservice"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey" //for Get
	ErrWrongServer = "ErrWrongServer"
	FAIL           = "Fail"
)

const (
	PUT    = "Put"
	APPEND = "Append"
)

func dprint(format string, args ...interface{}) {
	if viewservice.DEBUG == 1 {
		fmt.Printf(format, args...)
	}
}

type Err string
type PutAppend string

// Put or Append
type PutAppendArgs struct {
	Me    string
	Key   string
	Value string
	// You'll have to add definitions here.
	ID  int64
	Opt PutAppend

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type ForwordArgs struct {
	Key      string
	Value    string
	ID       int64
	Opt      PutAppend
	Me       string
	ClientMe string
}

type ForwordReply struct {
	Err Err
}

type InitArgs struct {
	Pairs    map[string]string
	LastInfo map[string]int64
	Me       string
}

type InitReply struct {
	Err Err
}
