package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	currentView *View

	pingHandlers [](func(*PingArgs) *PingArgs)

	servers     map[string]*StateInfo
	primaryView uint
}

type StateInfo struct {
	ping int
	view uint
}

/*
func (vs *ViewServer) PrimaryHandler(args *PingArgs) *PingArgs {
	if args.Me == vs.currentView.Primary {
		info := vs.servers[args.Me]
		info.ping = DeadPings
		if args.Viewnum == 0 {
			vs.IdleHandler(args)
			vs.SwitchHost("BTP")
		} else if vs.primaryView = args.Viewnum; vs.currentView.Backup == "" && len(vs.servers) > 2 {
			vs.SwitchHost("ITB")
		}
		return nil
	} else if vs.currentView.Primary == "" {
		vs.currentView.Primary = args.Me
		vs.servers[vs.currentView.Primary] = StateInfo{DeadPings, args.Viewnum}
		vs.primaryView = args.Viewnum
		vs.currentView.Viewnum++
		return nil
	} else {
		return args
	}
}

func (vs *ViewServer) BackupHandler(args *PingArgs) *PingArgs {
	if args.Me == vs.currentView.Backup {
		vs.servers[vs.currentView.Backup].ping = DeadPings
		return nil
	} else if vs.currentView.Backup == "" && vs.primaryView == vs.currentView.Viewnum {
		vs.currentView.Backup = args.Me
		vs.servers[vs.currentView.Backup].ping = DeadPings
		vs.currentView.Viewnum++
		return nil
	} else {
		return args
	}
}
*/
func (vs *ViewServer) PreHandler(args *PingArgs) *PingArgs {
	vs.PutToMap(args)
	if value, ok := vs.servers[vs.currentView.Primary]; ok && value.view != 0 {
		vs.primaryView = value.view
	}
	return args
}

func (vs *ViewServer) InitHandler(args *PingArgs) *PingArgs {
	//same as both primary and backup are dead
	if vs.currentView.Viewnum == 0 {
		vs.SwitchHost("ITP", "first ping")
		return nil
	}
	return args
}

func (vs *ViewServer) ViewHandler(args *PingArgs) *PingArgs {
	if args.Viewnum == 0 && vs.primaryView == vs.currentView.Viewnum {
		// a idle server request
		view := vs.currentView
		if args.Me == view.Primary {
			view.Primary = ""
			vs.SwitchHost("BTP", "primary 0 view")
		} else if args.Me == view.Backup {
			view.Backup = ""
			vs.SwitchHost("ITB", "bakcup 0 view")
		}
		return nil
	}
	return args
}

func (vs *ViewServer) NormalHandler(args *PingArgs) *PingArgs {
	if vs.currentView.Backup == "" && vs.currentView.Viewnum == vs.primaryView {
		vs.SwitchHost("ITB", "fill backup")
	}
	return nil
}

func (vs *ViewServer) PutToMap(args *PingArgs) {
	if value, ok := vs.servers[args.Me]; ok {
		value.ping = DeadPings
		value.view = args.Viewnum
		if args.Me == vs.currentView.Primary && args.Viewnum != 0 {
			vs.primaryView = args.Viewnum
		}
	} else {
		vs.servers[args.Me] = &StateInfo{DeadPings, args.Viewnum}
	}
}

func (vs *ViewServer) PrintInfo(args string) {
	if DEBUG == 1 {
		view := vs.currentView
		fmt.Println(args)
		fmt.Printf("primary is %s\n", view.Primary)
		fmt.Printf("backup is %s\n", view.Backup)
		for key, value := range vs.servers {
			fmt.Printf("%s is %d\n", key, value)
		}
		fmt.Printf("view id is %d, primary view is %d", vs.currentView.Viewnum, vs.primaryView)
		fmt.Println()
		fmt.Println()
	}
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code hereo
	vs.mu.Lock()
	defer vs.mu.Unlock()

	for _, handler := range vs.pingHandlers {
		res := handler(args)
		if res == nil {
			break
		}
	}
	*reply = PingReply{*vs.currentView}
	vs.PrintInfo("ping " + args.Me + " with " + strconv.Itoa(int(args.Viewnum)))
	return nil
}

//
// server Get() RPC handler.
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	*reply = GetReply{*vs.currentView}

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Your code here.
	for _, value := range vs.servers {
		value.ping--
	}

	if vs.primaryView != vs.currentView.Viewnum {
		return
	}

	// clear up dead servers
	for key, value := range vs.servers {
		if value.ping <= 0 {
			delete(vs.servers, key)
		}
	}
	_, ok1 := vs.servers[vs.currentView.Backup]
	if !ok1 {
		vs.currentView.Backup = ""
	}
	_, ok2 := vs.servers[vs.currentView.Primary]
	if !ok2 {
		vs.currentView.Primary = ""
	}

	primaryDie := vs.currentView.Primary == ""
	backupDie := vs.currentView.Backup == ""
	if primaryDie && backupDie {
		return
	}
	if !primaryDie && !backupDie {
		return
	}

	if backupDie {
		vs.SwitchHost("ITB", "tick")
	} else {
		vs.SwitchHost("BTP", "tick")
	}
	vs.PrintInfo("tick change view")
}

func (vs *ViewServer) SwitchHost(opt, from string) {
    if (DEBUG != 0) {
	    fmt.Println(from + ": switch server - " + opt + "\n")
    }
	view := vs.currentView
	isITB := true
	switch opt {
	case "ITP":
		view.Primary = ""
		for key := range vs.servers {
			view.Primary = key
			view.Viewnum++
			break
		}
	case "BTP":
		v, ok := vs.servers[vs.currentView.Backup]
		if ok {
			view.Primary = view.Backup
			vs.primaryView = v.view
			view.Backup = ""
			isITB = false
		} else {
			return
		}
		fallthrough
	case "ITB":
		for key := range vs.servers {
			if key != view.Primary {
				view.Backup = key
				break
			}
		}
		if isITB && view.Backup == "" {
			return
		} else {
			vs.currentView.Viewnum++
		}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currentView = &View{0, "", ""}
	vs.mu = sync.Mutex{}
	vs.servers = map[string]*StateInfo{}
	vs.pingHandlers = [](func(*PingArgs) *PingArgs){}
	vs.pingHandlers = append(vs.pingHandlers, vs.PreHandler)
	vs.pingHandlers = append(vs.pingHandlers, vs.InitHandler)
	vs.pingHandlers = append(vs.pingHandlers, vs.ViewHandler)
	vs.pingHandlers = append(vs.pingHandlers, vs.NormalHandler)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
