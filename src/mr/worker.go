package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type KeyValueWithReduceNum struct {
	kv        KeyValue
	reduceNum int
}

type ByReduceNum []KeyValueWithReduceNum

func (a ByReduceNum) Len() int           { return len(a) }
func (a ByReduceNum) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByReduceNum) Less(i, j int) bool { return a[i].reduceNum < a[j].reduceNum }

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	var args TaskArgs
	args.WorkerId = os.Getuid() + rand.Intn(100)
	for reply := GetTask(args); reply.Cmd != "please exit"; reply = GetTask(args) {
		args.Done = false
		if reply.Cmd == "wait" {
			time.Sleep(time.Second)
		} else if strings.HasPrefix(reply.Cmd, "reduce") {
			intermediate := []KeyValue{}
			reduceNum := strings.Split(reply.Cmd, "-")[1]
			files, err := filepath.Glob("./mr-*-" + reduceNum)
			if err != nil {
				fmt.Println(err)
			} else {
				for _, f := range files {
					jsonFile, err := os.Open(f)
					// if we os.Open returns an error then handle it
					if err != nil {
						fmt.Println(err)
					} else {
						dec := json.NewDecoder(jsonFile)
						for {
							var kv KeyValue
							if err := dec.Decode(&kv); err != nil {
								break
							}
							intermediate = append(intermediate, kv)
						}
						jsonFile.Close()
					}
				}
				sort.Sort(ByKey(intermediate))

				//
				// call Reduce on each distinct key in intermediate[],
				// and print the result to mr-out-0.
				//
				tmpfile, err := ioutil.TempFile("./", "")
				if err != nil {
					fmt.Println(err)
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

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)
					i = j
				}
				os.Rename(tmpfile.Name(), "./mr-out-"+reduceNum)
			}
			args.Done = true
		} else {
			reduceNum := reply.ReduceNumber
			filename := reply.Cmd

			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			kvaWithReduceNum := make(ByReduceNum, len(kva))
			for i := 0; i < len(kva); i++ {
				kvaWithReduceNum[i] = KeyValueWithReduceNum{kv: kva[i], reduceNum: ihash(kva[i].Key) % reduceNum}
			}
			sort.Sort(kvaWithReduceNum)
			i := 0
			for i < len(kvaWithReduceNum) {
				mapFileName := fmt.Sprintf("./mr-%d-%d", args.WorkerId, kvaWithReduceNum[i].reduceNum)
				currentKva := []KeyValue{}
				// jsonFile, err := os.Open(mapFileName)
				// // if we os.Open returns an error then handle it
				// if err != nil {
				// 	fmt.Println(err)
				// } else {
				// 	dec := json.NewDecoder(jsonFile)
				// 	for {
				// 		var kv KeyValue
				// 		if err := dec.Decode(&kv); err != nil {
				// 			break
				// 		}
				// 		currentKva = append(currentKva, kv)
				// 	}
				// 	jsonFile.Close()
				// }

				currentKva = append(currentKva, kvaWithReduceNum[i].kv)

				j := i + 1
				for j < len(kvaWithReduceNum) && kvaWithReduceNum[j].reduceNum == kvaWithReduceNum[i].reduceNum {
					currentKva = append(currentKva, kvaWithReduceNum[j].kv)
					j++
				}
				i = j
				tmpfile, err := ioutil.TempFile("./", "")
				if err != nil {
					fmt.Println(err)
				}
				enc := json.NewEncoder(tmpfile)
				for _, kv := range currentKva {
					err := enc.Encode(&kv)
					if err != nil {
						fmt.Println(err)
					}
				}
				input, err := ioutil.ReadFile(mapFileName)
				if err != nil {
					fmt.Println(err)
				} else {
					tmpfile.Write(input)
				}
				tmpfile.Close()
				os.Rename(tmpfile.Name(), mapFileName)
			}
			args.Done = true
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {
	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func GetTask(args TaskArgs) Task {
	reply := Task{}
	ok := call("Coordinator.GetTask", args, &reply)
	if ok {
		// fmt.Printf(reply.FileName)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
