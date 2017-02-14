package mapreduce

import (
	"fmt"
	"sync"
    "time"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
    todoch := make(chan int)
    go func() {
        for taskNumber := 0; taskNumber < ntasks; taskNumber++ {
            todoch <- taskNumber
        }
    }()

	wg := sync.WaitGroup{}
    count := ntasks // num of tasks remaining to run
    mu := sync.Mutex{}
    for {
        mu.Lock()
        cnt := count
        if count > 0 {
            count--
        }
        mu.Unlock()
        fmt.Printf("remain %v\n", cnt)
        if cnt == 0 {
            fmt.Println("in cnt == 0")
            wg.Wait()
            fmt.Println("after wg.wait", wg)
            mu.Lock()
            cnt := count
            mu.Unlock()
            if cnt == 0 {
                break
            } else {
                continue
            }
        }
        taskNumber := <-todoch
		args := DoTaskArgs{}
		args.JobName = jobName
		args.Phase = phase
		if phase == mapPhase {
			args.File = mapFiles[taskNumber]
		}
		args.TaskNumber = taskNumber
		args.NumOtherPhase = n_other
		worker := <-registerChan
        wg.Add(1)
		go func(id int) {
            state := call(worker, "Worker.DoTask", args, new(struct{}))
            // NOTE: if put wg.Done() after registerChan <- worker
            // it won't complete, because registerChan is unbuffered,
            // send to this channel will succeed only if there is a
            // receiver, so wg.Done() in the last thread corresponding to 
            // the two tasks will never be executed.

            // tricky, if put wg.Done directly after state, then if the task
            // failed, the main thread may not see the updated count
            if !state {
                mu.Lock()
                count++
                mu.Unlock()
                todoch <- id

            }
            wg.Done()
			if state {
                fmt.Printf("done task %v\n", id)
                registerChan <- worker
            }
            fmt.Printf("finish task %v\n", id)
		}(taskNumber)
	}
    time.Sleep(2 * time.Second)
    fmt.Println("waitgroup is ", wg)
	fmt.Printf("Schedule: %v phase done\n", phase)
}
