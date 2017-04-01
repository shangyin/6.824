package pbservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"viewservice"
)

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk

	// Your declarations here.

	// two bool vars are for the optimization of comparasion
	view      *viewservice.View
	isPrimary bool
	isBackup  bool

	pairs map[string]string
	// use for PutAppend
	lastInfo map[string]int64
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.

	//TODO judge state
	//need to judge request id here?
	//maybe Get doesn't need at-most-once

	*reply = GetReply{}

	if !pb.isPrimary {
		reply.Err = ErrWrongServer
	}

	pb.mu.Lock()
	defer pb.mu.Unlock()

	value, ok := pb.pairs[args.Key]
	fArgs := &ForwordArgs{}
	fReply := &ForwordReply{}
	if pb.view.Backup != "" {
		count := 0
		fOk := call(pb.view.Backup, "PBServer.Forword", fArgs, fReply)
		for !fOk && count <= 5 {
			fOk = call(pb.view.Backup, "PBServer.Forword", fArgs, fReply)
			count++
		}
		if !fOk || fReply.Err == ErrWrongServer {
			reply.Value = ""
			reply.Err = ErrWrongServer
			return nil
		}
	}

	if args.Key == "a" && value == "1" {
		dprint("Fuck you!\n")
	}

	if ok {
		reply.Value = value
		reply.Err = OK
	} else {
		reply.Value = ""
		reply.Err = ErrNoKey
	}

	//problem: does it need to sychronize with bakcup?
	//no forword may make myself keep thinking I am a primary
	//no, I will tick to update view
	//so Get() don't need to forword

	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here

	pb.mu.Lock()
	defer pb.mu.Unlock()

	dprint("pbserver %s, primary is %s, backup is %s\n", pb.me, pb.view.Primary, pb.view.Backup)

	// dprint("pbserver: starts %s %s - %s\n", args.Opt, args.Key, args.Value)

	// dprint("%s : start %s %s - %s\n", pb.me, args.Opt, args.Key, args.Value)
	// defer dprint("%s : end %s %s - %s\n", pb.me, args.Opt, args.Key, args.Value)

	// why client can communicate with a non-primary server
	// A: primary becomes idle, even quickly becomes backup
	// which means it has been dead

	if !pb.isPrimary {
		reply.Err = ErrWrongServer
		return nil
	}

	//ignore the same request
	id, ok := pb.lastInfo[args.Me]
	if ok && args.ID == id {
		reply.Err = OK
		// dprint("pbserver denied %s - %s\n", args.Key, args.Value)
		return nil
	}

	// forword to backup
	// reply to client until sychronize to backup successfully
	// Done: but what if forword responses a ErrWrongServer?
	//
	if pb.view.Backup != "" {
		fArgs := &ForwordArgs{args.Key, args.Value, args.ID, args.Opt, pb.me, args.Me}
		fRely := &ForwordReply{}
		fOk := call(pb.view.Backup, "PBServer.Forword", fArgs, fRely)
		count := 1
		for true {
			if !fOk {
				if count += 1; count >= 5 {
					reply.Err = ErrWrongServer
					return nil
				}
				fOk = call(pb.view.Backup, "PBServer.Forword", fArgs, fRely)
			} else if fRely.Err == ErrWrongServer {
				// now this is idle server
				pb.isPrimary = false
				reply.Err = ErrWrongServer
				return nil
			} else {
				// fOk is true
				break
			}
		}
	}

	value, ok := pb.pairs[args.Key]
	if args.Opt == PUT || !ok {
		pb.pairs[args.Key] = args.Value
		reply.Err = OK
	} else {
		pb.pairs[args.Key] = value + args.Value
		reply.Err = OK
	}

	//update last request's info
	pb.lastInfo[args.Me] = args.ID
	// dprint("pbserver: finishs %s %s - %s\n", args.Opt, args.Key, args.Value)
	return nil
}

func (pb *PBServer) Forword(args *ForwordArgs, reply *ForwordReply) error {

	pb.mu.Lock()
	defer pb.mu.Unlock()
	// dprint("forword starts from %s to %s, %s - %s\n", args.Me, pb.me, args.Key, args.Value)

	// defer dprint("forword: end %s to %s\n", args.Me, pb.me)

	if !pb.isBackup {
		reply.Err = ErrWrongServer
		return nil
	} else if args.Opt == "GET" {
		reply.Err = OK
		return nil
	}

	// lastInfo have to be same as primary
	// example:
	id, ok := pb.lastInfo[args.ClientMe]
	if ok && id == args.ID {
		reply.Err = OK
		dprint("pbserver denied %s - %s\n", args.Key, args.Value)
		return nil
	}

	//TODO: handle ID issue
	//is that a pbservice only need A lastID
	//or two lastID for outer request and inner request

	//update map
	value, ok := pb.pairs[args.Key]
	if args.Opt == PUT || !ok {
		pb.pairs[args.Key] = args.Value
		reply.Err = OK
	} else {
		pb.pairs[args.Key] = value + args.Value
		reply.Err = OK
	}

	pb.lastInfo[args.ClientMe] = args.ID
	// dprint("forword finishs from %s to %s, %s - %s\n", args.Me, pb.me, args.Key, args.Value)
	return nil
}

func (pb *PBServer) Init(args *InitArgs, reply *InitReply) error {
	//TODO
	// only invokes when it becomes backup (from idle or directly)
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if !pb.isBackup {
		*reply = InitReply{ErrWrongServer}
		return nil
	}

	*reply = InitReply{OK}
	pb.pairs = args.Pairs
	pb.lastInfo = args.LastInfo // it's supposed to be same as primary

	// dprint("%s inits invoked by %s\n", pb.me, args.Me)
	// for key, value := range pb.pairs {
	// dprint("%s - %s\n", key, value)
	// }

	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	v, err := pb.vs.Ping(pb.view.Viewnum)
	if err != nil {
		dprint("PBServer %s : %s\n", pb.me, err)
		return
	}
	// dprint("%s start tick\n", pb.me)
	// defer dprint("%s end tick\n", pb.me)

	used := pb.view.Backup
	pb.view = &v
	pb.isPrimary = v.Primary == pb.me
	pb.isBackup = v.Backup == pb.me

	if pb.isPrimary && used != v.Backup && v.Backup != "" {
		args := &InitArgs{pb.pairs, pb.lastInfo, pb.me}
		reply := &InitReply{}
		ok := call(pb.view.Backup, "PBServer.Init", args, reply)
		count := 0
		for !ok {
			if count += 1; count >= 5 {
				dprint("tick: fail to init backup\n")
				break
			}
			ok = call(pb.view.Backup, "PBServer.Init", args, reply)
		}
		if !ok || reply.Err == ErrWrongServer {
			pb.view.Backup = ""
		}
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.pairs = make(map[string]string)
	pb.lastInfo = make(map[string]int64)
	pb.view = &viewservice.View{}
	pb.isPrimary = false
	pb.isBackup = false

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		//now pb is dead
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
