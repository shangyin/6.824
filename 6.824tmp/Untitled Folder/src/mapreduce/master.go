package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	finishedJobs := 0
	finishedSignal := make(chan int, mr.nMap+mr.nReduce)
	leftMapCh := make(chan int, mr.nMap)
	leftReduceCh := make(chan int, mr.nReduce)

	for i := 0; i < mr.nMap; i++ {
		leftMapCh <- i
	}
	for i := 0; i < mr.nReduce; i++ {
		leftReduceCh <- i
	}

	go func() {
		for {
			newWorker := <-mr.registerChannel
			mr.Workers[newWorker] = &WorkerInfo{newWorker}
			mr.somebodyFree <- mr.Workers[newWorker]
		}
	}()

MapEnd:
	for {
		select {
		case mapJob := <-leftMapCh:
			fWorker := <-mr.somebodyFree
			var args DoJobArgs
			args.File = mr.file
			args.Operation = Map
			args.JobNumber = mapJob
			args.NumOtherPhase = mr.nReduce
			var reply DoJobReply
			go func() {
				ok := call(fWorker.address, "Worker.DoJob", &args, &reply)
//				ok = true
				if ok {
					finishedSignal <- 1
					mr.somebodyFree <- fWorker
				} else {
					leftMapCh <- args.JobNumber
				}
			}()
		case finished := <-finishedSignal:
			finishedJobs += finished
			if finishedJobs == mr.nMap {
				break MapEnd
			}
		}
	}

ReduceEnd:
	for {
		select {
		case reduceNum := <-leftReduceCh:
			fWorker := <-mr.somebodyFree
			var args DoJobArgs
			args.File = mr.file
			args.Operation = Reduce
			args.JobNumber = reduceNum
			args.NumOtherPhase = mr.nMap
			var reply DoJobReply
			go func() {
				ok := call(fWorker.address, "Worker.DoJob", &args, &reply)
//				ok = true
				if ok {
					finishedSignal <- 1
					mr.somebodyFree <- fWorker
				} else {
					leftReduceCh <- args.JobNumber
				}
			}()
		case finished := <-finishedSignal:
			finishedJobs += finished
			if finishedJobs == mr.nMap+mr.nReduce {
				break ReduceEnd
			}
		}
	}
	return mr.KillWorkers()
}
