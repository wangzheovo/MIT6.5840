package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import  "fmt"
import "sync"
import "time"
import "io/ioutil"
import "strings"
import "strconv"

var mu sync.Mutex

type Job struct{
	JobType JobType
	InputFile []string
	JobId int
	ReducerNum int
}

type JobMetaInfo struct {
	condition JobCondition
	StartTime time.Time
	JobPtr *Job
}

type JobMetaHolder struct {
	MetaMap map[int]*JobMetaInfo
}

type Coordinator struct {
	// Your definitions here.
	JobChannelMap chan *Job
	JobChannelReduce chan *Job
	ReducerNum int
	MapNum int
	uniqueJobId int
	CoordinatorCondition Condition
	JobMetaHolder JobMetaHolder
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) DistributeJob(args *ExampleArgs, reply *Job) error{
	mu.Lock()
	defer mu.Unlock()
	fmt.Println("coordinator get a request from worker: ")
	if c.CoordinatorCondition == MapPhase {
	    if len(c.JobChannelMap) > 0{
			*reply = *<-c.JobChannelMap
			if !c.JobMetaHolder.fireJob(reply.JobId){
				fmt.Printf("[duplocated job] job %d is running\n", reply.JobId)
			}
		}else{
			reply.JobType = WaitingJob
			if c.JobMetaHolder.checkMapJobDone(){
				c.nextPhase()
			}
			return nil
		}
	}else if c.CoordinatorCondition == ReducePhase{
		if len(c.JobChannelReduce) > 0{
			*reply = *<-c.JobChannelReduce
			if !c.JobMetaHolder.fireJob(reply.JobId){
				fmt.Printf("job %d is running\n", reply.JobId)
			}
		}else{
			reply.JobType = WaitingJob
			if c.JobMetaHolder.checkReduceJobDone(){
				c.nextPhase()
			}
			return nil
		}
	}else{
		reply.JobType = KillJob
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

	// Your code here.
	mu.Lock()
	defer mu.Unlock()
	fmt.Println("+++++++++++++++++++++++++++++++++++++++++++")
	return c.CoordinatorCondition == AllDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		JobChannelMap: make(chan *Job, len(files)),
		JobChannelReduce: make(chan *Job, nReduce),
		JobMetaHolder: JobMetaHolder{
		    MetaMap: make(map[int]*JobMetaInfo, len(files)+nReduce),
		},
		CoordinatorCondition: MapPhase,
		ReducerNum: nReduce,
		MapNum: len(files),
		uniqueJobId: 0,
	}
	
	fmt.Println("make coordinator")

	c.makeMapJobs(files)

	c.server()

	go c.CrashHandler()
	return &c
}

func(c *Coordinator) makeMapJobs(file []string){
	for _, f := range file{
	    id := c.generateJobId()
		job := Job{
			JobType : MapJob,
			InputFile: []string{f},
			JobId: id,
			ReducerNum: c.ReducerNum,
		}
		jobMetaINfo := JobMetaInfo{
			condition: JobWaiting,
			JobPtr:    &job,
		}
		c.JobMetaHolder.putJob(&jobMetaINfo)
		fmt.Println("making map job :", &job)
		c.JobChannelMap <- &job
		
		
	}
	fmt.Println("all map job make done")
	c.JobMetaHolder.checkMapJobDone()
}

func(c *Coordinator) makeReduceJobs(){
    for i := 0; i < c.ReducerNum; i++{
		id := c.generateJobId()
		fmt.Println("make reduce job :", id)
		JobTodo := Job{
			JobType : ReduceJob,
			JobId: id,
			InputFile: TmpFileAssignHelper(i,"main/mr-tmp"),
		}
		jobMetaInfo := JobMetaInfo{
		    condition: JobWaiting,
			JobPtr:    &JobTodo,
		}
		c.JobMetaHolder.putJob(&jobMetaInfo)
		c.JobChannelReduce <- &JobTodo
	}
	fmt.Println("all reduce job make done")
	c.JobMetaHolder.checkReduceJobDone()
}

func TmpFileAssignHelper(whichReduce int, tmpFileDirectoryName string) []string {
	var res []string
	path, _ := os.Getwd()
	rd, _ := ioutil.ReadDir(path)
	for _, fi := range rd {
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(whichReduce)) {
			res = append(res, fi.Name())
		}
	}
	return res
}

func (j *JobMetaHolder) putJob(JobInfo *JobMetaInfo) bool {
	jobId := JobInfo.JobPtr.JobId
	meta, _ := j.MetaMap[jobId]
	if meta != nil {
		fmt.Println("meta contains job which id = ", jobId)
		return false
	} else {
		j.MetaMap[jobId] = JobInfo
	}
	return true
}

func (c *Coordinator) generateJobId() int {
	res := c.uniqueJobId
	c.uniqueJobId++
	return res
}

func (j *JobMetaHolder) getJobMetaInfo(jobId int) (bool, *JobMetaInfo){
	res, ok := j.MetaMap[jobId]
	return ok, res
}

func (j *JobMetaHolder) fireJob(jobId int) bool{
	ok, jobInfo := j.getJobMetaInfo(jobId)
	if !ok || jobInfo.condition != JobWaiting {
	    return false
	}
	jobInfo.condition = JobWorking
	jobInfo.StartTime = time.Now()
	return true

}

func (j *JobMetaHolder) checkMapJobDone() bool {

	mapDoneNum := 0
	mapUndoneNum := 0
	for _, v := range j.MetaMap {
		if v.JobPtr.JobType == MapJob {
			if v.condition == JobDone {
				mapDoneNum++
			} else {
				mapUndoneNum++
			}
		}
	}
	fmt.Printf("%d/%d map jobs are done\n",
		mapDoneNum, mapDoneNum+mapUndoneNum)
	// 
	if (mapDoneNum > 0 && mapUndoneNum == 0) {
		return true
	}

	return false
}

func (j *JobMetaHolder) checkReduceJobDone() bool {
	reduceDoneNum := 0
	reduceUndoneNum := 0

	for _, v := range j.MetaMap {
		if v.JobPtr.JobType == ReduceJob{
			if v.condition == JobDone {
				reduceDoneNum++
			} else {
				reduceUndoneNum++
			}
		}
	}
	fmt.Printf(" %d/%d reduce job are done\n",
		reduceDoneNum, reduceDoneNum+reduceUndoneNum)
	// 
	if (reduceDoneNum > 0 && reduceUndoneNum == 0)  {
		return true
	}

	return false
}


func (j *JobMetaHolder) checkJobDone() bool {
	reduceDoneNum := 0
	reduceUndoneNum := 0
	mapDoneNum := 0
	mapUndoneNum := 0
	for _, v := range j.MetaMap {
		if v.JobPtr.JobType == MapJob {
			if v.condition == JobDone {
				mapDoneNum++
			} else {
				mapUndoneNum++
			}
		} else {
			if v.condition == JobDone {
				reduceDoneNum++
			} else {
				reduceUndoneNum++
			}
		}
	}
	fmt.Printf("%d/%d map jobs are done, %d/%d reduce job are done\n",
		mapDoneNum, mapDoneNum+mapUndoneNum, reduceDoneNum, reduceDoneNum+reduceUndoneNum)
	// 
	if (reduceDoneNum > 0 && reduceUndoneNum == 0) || (mapDoneNum > 0 && mapUndoneNum == 0) {
		return true
	}

	return false
}

func (c *Coordinator) nextPhase(){
	if c.CoordinatorCondition == MapPhase {
	    c.makeReduceJobs()
		c.CoordinatorCondition = ReducePhase
	}else if c.CoordinatorCondition == ReducePhase {
		c.CoordinatorCondition = AllDone
	}
}

func (c *Coordinator) JobIsDone(args *Job, reply *ExampleReply) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.JobType {
	case MapJob:
		ok, meta := c.JobMetaHolder.getJobMetaInfo(args.JobId)
		//prevent a duplicated work which returned from another worker
		if ok && meta.condition == JobWorking {
			meta.condition = JobDone
			fmt.Printf("Map task on %d complete\n", args.JobId)
		} else {
			fmt.Println("[duplicated] job done", args.JobId)
		}
		break
	case ReduceJob:
		fmt.Printf("Reduce task on %d complete\n", args.JobId)
		ok, meta := c.JobMetaHolder.getJobMetaInfo(args.JobId)
		//prevent a duplicated work which returned from another worker
		if ok && meta.condition == JobWorking {
			meta.condition = JobDone
		} else {
			fmt.Println("[duplicated] job done", args.JobId)
		}
		break
	default:
		panic("wrong job done")
	}
	return nil

}

func (c *Coordinator) CrashHandler() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		// if c.CoordinatorCondition == AllDone {
		// 	mu.Unlock()
		// }

		timenow := time.Now()
		for _, v := range c.JobMetaHolder.MetaMap {
			// fmt.Println(v)
			if v.condition == JobWorking {
				fmt.Println("job", v.JobPtr.JobId, " working for ", timenow.Sub(v.StartTime))
			}
			if v.condition == JobWorking && time.Now().Sub(v.StartTime) > 5*time.Second {
				fmt.Println("detect a crash on job ", v.JobPtr.JobId)
				switch v.JobPtr.JobType {
				case MapJob:
					c.JobChannelMap <- v.JobPtr
					v.condition = JobWaiting
				case ReduceJob:
					c.JobChannelReduce <- v.JobPtr
					v.condition = JobWaiting

				}
			}
		}
		mu.Unlock()
	}

}