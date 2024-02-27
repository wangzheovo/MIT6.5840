package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import  "fmt"
import "sync"
import "time"

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
	// CoordinatorCondition Condition
	// JobMetaHolder JobMetaHolder
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) DistributeJob(args *ExampleArgs, reply *Job) error{
	*reply = *<-c.JobChannelMap
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
	ret := false

	// Your code here.


	return ret
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
		// jobMetaHolder: JobMetaHolder{
		//     MetaMap: make(map[int]*JobMetaInfo, len(files)+nReduce),
		// },
		// CoordinatorCondition: MapPhase,
		ReducerNum: nReduce,
		MapNum: len(files),
		uniqueJobId: 0,
	}

	// Your code here.
	
	fmt.Println("make coordinator")



	
	c.makeMapJobs(files)

	c.server()

	// go c.CrashHandler
	return &c
}

func(c *Coordinator) makeMapJobs(file []string){
	for _, f := range file{
	    // id := c.generateJobId()
		id := 1
		job := Job{
			JobType : MapJob,
			InputFile: []string{f},
			JobId: id,
			ReducerNum: c.ReducerNum,
		}
		// jobMetaINfo := JobMetaInfo{
		// 	condition: JobWaiting,
		// 	JobPtr:    &job,
		// }
		// c.jobMetaHolder.putJob(&jobMetaINfo)
		fmt.Println("making map job :", &job)
		c.JobChannelMap <- &job
		
		
	}
	fmt.Println("all map job done")
	// c.jobMetaHolder.checkJobDone()
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

func (j *JobMetaHolder) fireJob(jobId int) nool{
	ok, jobInfo := j.getJob(jobId)
	if !ok || jobInfo,condition != JobWaiting {
	    return false
	}
	jobInfo.condition = JobWorking
	jobInfo.StartTime = time.Now()
	return true

}

func (j *JobMetaHolder) checkJobDone() bool {
	reduceDoneNum := 0
	reduceUndoneNum := 0
	mapDoneNum := 0
	mapUndoneNum := 0
	for _, v := range j.MetaMap {
		if v.JobPtr.JobType == MapJob {
			if v.condition == JobDone {
				mapDoneNum += 1
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

	if (reduceDoneNum > 0 && reduceUndoneNum == 0) || (mapDoneNum > 0 && mapUndoneNum == 0) {
		return true
	}

	return false
}

