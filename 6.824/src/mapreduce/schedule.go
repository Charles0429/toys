package mapreduce

import "fmt"

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type taskStat struct {
	taskNumber int
	ok         bool
}

func (mr *Master) checkWorkerExist(w string) bool {
	mr.Lock()
	defer mr.Unlock()
	for _, v := range mr.workers {
		if w == v {
			return true
		}
	}
	return false
}

func (mr *Master) chooseTask(failedTasks []int, nTaskIndex int) ([]int, int) {
	if 0 == len(failedTasks) {
		return failedTasks, nTaskIndex
	} else {
		fmt.Println("choose failed tasks")
		task := failedTasks[0]
		failedTasks = failedTasks[1:len(failedTasks)]
		fmt.Println("failedTasks", failedTasks)
		return failedTasks, task
	}
}

func (mr *Master) runMapTask(nTaskNumber int, w string, doneTask chan taskStat) {
	if nTaskNumber < len(mr.files) {
		var args DoTaskArgs
		args.JobName = mr.jobName
		args.File = mr.files[nTaskNumber]
		args.Phase = mapPhase
		args.TaskNumber = nTaskNumber
		args.NumOtherPhase = mr.nReduce
		go func() {
			ok := call(w, "Worker.DoTask", args, new(struct{}))
			var taskstat taskStat
			taskstat.taskNumber = args.TaskNumber
			taskstat.ok = ok
			if ok {
				doneTask <- taskstat
				fmt.Println("done task %d", args.TaskNumber)
			} else {
				doneTask <- taskstat
				fmt.Println("get failed task %d", args.TaskNumber)
			}
		}()
	} else {
		fmt.Printf("all tasks sent out")
	}
}

func (mr *Master) runReduceTask(nTaskNumber int, w string, doneTask chan taskStat) {
	if nTaskNumber < mr.nReduce {
		var args DoTaskArgs
		args.JobName = mr.jobName
		args.Phase = reducePhase
		args.TaskNumber = nTaskNumber
		args.NumOtherPhase = len(mr.files)
		go func() {
			ok := call(w, "Worker.DoTask", args, new(struct{}))
			var taskstat taskStat
			taskstat.taskNumber = args.TaskNumber
			taskstat.ok = ok
			if ok {
				doneTask <- taskstat
				fmt.Println("done task %d", args.TaskNumber)
			} else {
				doneTask <- taskstat
				fmt.Println("failed task %d", args.TaskNumber)
			}
		}()
	} else {
		fmt.Printf("all task sent out")
	}
}

func (mr *Master) scheduleMap() {
	fmt.Println("begin scheduling map tasks")
	taskWorkerMap := make(map[int]string)
	doneTask := make(chan taskStat, 1)
	var nTaskIndex = 0
	var failedTasks []int
	mr.Lock()
	var nInitTask = min(len(mr.files), len(mr.workers))
	mr.Unlock()
	for ; nTaskIndex < nInitTask; nTaskIndex++ {
		mr.Lock()
		w := mr.workers[nTaskIndex]
		mr.Unlock()
		mr.runMapTask(nTaskIndex, w, doneTask)
	}

	for {
		select {
		case newWorker := <-mr.registerChannel:
			fmt.Println("New Worker register %s", newWorker)
			var nextTask int
			failedTasks, nextTask = mr.chooseTask(failedTasks, nTaskIndex)
			if nextTask < len(mr.files) {
				fmt.Println("nextTask %d, total %d", nextTask, len(mr.files))
				mr.runMapTask(nextTask, newWorker, doneTask)
				taskWorkerMap[nextTask] = newWorker
				if nTaskIndex == nextTask {
					nTaskIndex++
				}
			}
		case taskStat := <-doneTask:
			var w string
			taskNumber, ok := taskStat.taskNumber, taskStat.ok
			if !ok {
				failedTasks = append(failedTasks, taskNumber)
			} else {
				w = taskWorkerMap[taskNumber]
				delete(taskWorkerMap, taskNumber)
			}
			if mr.checkWorkerExist(w) {
				fmt.Println("failed task count, failed tasks", len(failedTasks), failedTasks)
				var nextTask int
				failedTasks, nextTask = mr.chooseTask(failedTasks, nTaskIndex)
				if nextTask < len(mr.files) {
					fmt.Println("failed task count, failed tasks", len(failedTasks), failedTasks)
					fmt.Println("nextTask %d, total %d", nextTask, len(mr.files))
					mr.runMapTask(nextTask, w, doneTask)
					taskWorkerMap[nextTask] = w
					if nTaskIndex == nextTask {
						nTaskIndex++
					}
				}
			}
			fmt.Println("task index %d, task number %d, map count", nTaskIndex, taskNumber, len(taskWorkerMap))
			for k, v := range taskWorkerMap {
				fmt.Println("%s:%v", k, v)
			}
			if (nTaskIndex == len(mr.files)) && (0 == len(taskWorkerMap)) {
				fmt.Println("all tasks in mapPhase is done")
				return
			}
		}

	}
}

func (mr *Master) scheduleReduce() {
	fmt.Println("start scheduling reduce tasks")
	taskWorkerMap := make(map[int]string)
	doneTask := make(chan taskStat, 1)
	var failedTasks []int
	var nTaskIndex = 0
	mr.Lock()
	var nInitTask = min(mr.nReduce, len(mr.workers))
	mr.Unlock()
	for ; nTaskIndex < nInitTask; nTaskIndex++ {
		mr.Lock()
		w := mr.workers[nTaskIndex]
		mr.Unlock()
		taskWorkerMap[nTaskIndex] = w
		mr.runReduceTask(nTaskIndex, w, doneTask)
	}

	for {
		select {
		case newWorker := <-mr.registerChannel:
			fmt.Println("New Worker register %s", newWorker)
			var nextTask int
			failedTasks, nextTask = mr.chooseTask(failedTasks, nTaskIndex)
			if nextTask < mr.nReduce {
				fmt.Println("nextTask %d, total %d", nextTask, len(mr.files))
				mr.runReduceTask(nextTask, newWorker, doneTask)
				taskWorkerMap[nextTask] = newWorker
				if nTaskIndex == nextTask {
					nTaskIndex++
				}
			}
		case taskStat := <-doneTask:
			var w string
			taskNumber, ok := taskStat.taskNumber, taskStat.ok
			if !ok {
				failedTasks = append(failedTasks, taskNumber)
			} else {
				w = taskWorkerMap[taskNumber]
				delete(taskWorkerMap, taskNumber)
			}
			if mr.checkWorkerExist(w) {
				var nextTask int
				failedTasks, nextTask = mr.chooseTask(failedTasks, nTaskIndex)
				if nextTask < mr.nReduce {
					fmt.Println("nextTask %d, total %d", nextTask, len(mr.files))
					mr.runReduceTask(nextTask, w, doneTask)
					taskWorkerMap[nextTask] = w
					if nTaskIndex == nextTask {
						nTaskIndex++
					}
				}
			}
			fmt.Println("task index %d, task number %d, map count", nTaskIndex, taskNumber, len(taskWorkerMap))
			for k, v := range taskWorkerMap {
				fmt.Println("%s:%v", k, v)
			}
			if (nTaskIndex == mr.nReduce) && (0 == len(taskWorkerMap)) {
				fmt.Println("all tasks in mapPhase is done")
				return
			}
		}
	}
}

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	switch phase {
	case mapPhase:
		mr.scheduleMap()
	case reducePhase:
		mr.scheduleReduce()
	}
	fmt.Printf("Schedule: %v phase done\n", phase)
}
