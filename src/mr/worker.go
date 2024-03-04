package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "strconv"
import "encoding/json"
import "time"
import "sort"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, id string) {

	// Your worker implementation here.
	workerId := id
	alive := true
	attemp := 0
	for alive{
		attemp++
		fmt.Println(workerId, " attemp:",attemp)
		job := RequireTask(workerId)
		
		fmt.Println(workerId, " worker get job:",job)

		switch job.JobType {
			case MapJob:
				{
					DoMap(mapf,job)
					fmt.Println("do map",job.JobId)
					JobIsDone(workerId,job)
				}
			case ReduceJob:
				{
					if job.JobId >= 8{
						DoReduce(reducef,job)
						fmt.Println("do reduce",job.JobId)
						JobIsDone(workerId,job)
					}
				}
			case WaitingJob:
				{
					fmt.Println("waiting job")
					time.Sleep(time.Second)
				}
			case KillJob:
				{
					time.Sleep(time.Second)
					fmt.Println("[Statis] : ",  workerId, "terminated")
					alive = false
				}
		}
		time.Sleep(time.Second)
	}


}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}


func DoMap(mapf func(string,string) []KeyValue, response *Job){
	var intermediate []KeyValue
	filename := response.InputFile[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate = mapf(filename, string(content))
	// sort.Sort(ByKey(intermediate))
	rNum := response.ReducerNum
	HashedKV := make([][]KeyValue, rNum)
	for _, kv := range intermediate {
	    HashedKV[ihash(kv.Key)%rNum] = append(HashedKV[ihash(kv.Key)%rNum], kv)
	}
	for i := 0; i < rNum; i++ {
	    oname := "mr-tmp-" + strconv.Itoa(response.JobId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		// json编码，用于网络传输
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			enc.Encode(&kv)
		}
		ofile.Close()
	}
}

func RequireTask(workerId string) *Job {
	args := ExampleArgs{}

	reply := Job{}

	call("Coordinator.DistributeJob", &args, &reply)
	fmt.Println("get response", &reply)

	return &reply

}

func JobIsDone(workerId string, job *Job){
	args := job
	reply := &ExampleReply{}
	call("Coordinator.JobIsDone", &args, &reply)
}


func DoReduce(reducef func(string, []string) string, response *Job){
	reduceFileNum := response.JobId
	intermediate := readFromLocalFile(response.InputFile)
	sort.Sort(ByKey(intermediate))
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(tempFile.Name(), oname)
}


func readFromLocalFile(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return kva
}

