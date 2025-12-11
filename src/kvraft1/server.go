package kvraft

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

func serverDbgPrint(kv *KVServer, args ...interface{}) {
	timeStr := time.Now().Format("15:04:05.000")
	msg := fmt.Sprintln(args...)
	fmt.Printf("[KV] me=%d | %s [%s]\n", kv.me, strings.TrimSpace(msg), timeStr)
}

type Val struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	data map[string]Val
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch T := req.(type) {
	case rpc.GetArgs:
					if val, ok := kv.data[T.Key]; ok {
			return rpc.GetReply{Value: val.Value, Version: val.Version, Err: rpc.OK}
		} else {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
	case *rpc.GetArgs:
		if val, ok := kv.data[T.Key]; ok {
			return rpc.GetReply{Value: val.Value, Version: val.Version, Err: rpc.OK}
		} else {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
	case rpc.PutArgs:

		val, ok := kv.data[T.Key]
		if !ok {
			if T.Version != 0 {
				return rpc.PutReply{Err: rpc.ErrNoKey}
			}
			kv.data[T.Key] = Val{Value: T.Value, Version: 1}
			serverDbgPrint(kv, "put for first time",T.Key )
			return rpc.PutReply{Err: rpc.OK}
		}

		if val.Version != T.Version {
			serverDbgPrint(kv, "put rpcVersion",T.Key, T.Version, "but actual version", val.Version)
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		kv.data[T.Key] = Val{Value: T.Value, Version: T.Version+1}
		return rpc.PutReply{Err: rpc.OK}
	case *rpc.PutArgs:
		val, ok := kv.data[T.Key]
		if !ok {
			if T.Version != 0 {
				return rpc.PutReply{Err: rpc.ErrNoKey}
			}
			kv.data[T.Key] = Val{Value: T.Value, Version: 1}
			serverDbgPrint(kv, "put for first time",T.Key )
			return rpc.PutReply{Err: rpc.OK}
		}

		if val.Version != T.Version {
			serverDbgPrint(kv, "put rpcVersion",T.Key, T.Version, "but actual version", val.Version)
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		kv.data[T.Key] = Val{Value: T.Value, Version: T.Version+1}
		return rpc.PutReply{Err: rpc.OK}
	default:
		fmt.Printf("Type of req: %T\n", req)
		panic("wrong arg type")
		// Your code here
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	serverDbgPrint(kv, "Get:", fmt.Sprintf("%+v", *args))
	ok, result := kv.rsm.Submit(args)
	if ok != rpc.OK {
		reply.Err = ok
		return
	}
	rep := result.(rpc.GetReply)
	reply.Value = rep.Value
	reply.Version = rep.Version
	reply.Err = rep.Err
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	serverDbgPrint(kv, "Put:", fmt.Sprintf("%+v", *args))
	args.From = kv.me
	errCode, result := kv.rsm.Submit(args)
		if result != nil {

	serverDbgPrint(kv, "Put results:", errCode, fmt.Sprintf("%+v",result.(rpc.PutReply) ))
		}
	if errCode != rpc.OK {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	rep := result.(rpc.PutReply)
	reply.Err = rep.Err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetReply{})

	kv := &KVServer{me: me}
	kv.data = make(map[string]Val)

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}