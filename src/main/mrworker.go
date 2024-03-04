package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import "6.5840/mr"
import "plugin"
import "os"
// import "fmt"
import "log"

func main() {
	// if len(os.Args) != 3 {
	// 	fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so workerId\n")
	// 	os.Exit(1)
	// }

	mapf, reducef := loadPlugin(os.Args[1])
	// workerId := os.Args[2]
	// mr.Worker(mapf, reducef, workerId)
	mr.Worker(mapf, reducef, "1")
	mr.Worker(mapf, reducef, "2")
	mr.Worker(mapf, reducef, "3")
	mr.Worker(mapf, reducef, "4")
	mr.Worker(mapf, reducef, "5")
	mr.Worker(mapf, reducef, "6")
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
