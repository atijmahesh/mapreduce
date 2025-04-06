package mapreduce

import (
	"container/list"
	"fmt"
	"sync"
)

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
	var wg sync.WaitGroup
	workerChannel := make(chan string, 10)

	// Handle worker reg in separate goroutine
	go func() {
		for worker := range mr.registerChannel {
			DPrintf("Master: Registered worker %s\n", worker)
			workerChannel <- worker
		}
	}()

	// Schedule all map tasks
	for i := 0; i < mr.nMap; i++ {
		mr.assign(Map, i, mr.nReduce, &wg, workerChannel)
	}
	wg.Wait()

	// Schedule all reduce tasks
	for i := 0; i < mr.nReduce; i++ {
		mr.assign(Reduce, i, mr.nMap, &wg, workerChannel)
	}
	wg.Wait()

	return mr.KillWorkers()
}

func (mr *MapReduce) assign(jobType JobType, jobNumber int, numOtherPhase int, wg *sync.WaitGroup, workerChannel chan string) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		args := DoJobArgs{
			File:          mr.file,
			Operation:     jobType,
			JobNumber:     jobNumber,
			NumOtherPhase: numOtherPhase,
		}
		var reply DoJobReply

		for {
			select {
			case worker := <-workerChannel:
				DPrintf("Assigning %s job %d to worker %s\n", jobType, jobNumber, worker)
				if call(worker, "Worker.DoJob", &args, &reply) {
					// job succeeded
					workerChannel <- worker
					return
				}
				// job failed
				fmt.Printf("Failed to assign %s job %d to worker %s, retrying...\n", jobType, jobNumber, worker)
			}
		}
	}()
}
