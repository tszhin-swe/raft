package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"strings"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

func clerkDbgPrint(ck *Clerk, args ...interface{}) {
	timeStr := time.Now().Format("15:04:05.000")
	msg := fmt.Sprintln(args...)
	fmt.Printf("[CLNT] id=%d | %s [%s]\n", ck.id, strings.TrimSpace(msg), timeStr)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	last_leader int
	id          int64
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.last_leader = 0
	ck.id = nrand()
	// You'll have to add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for first := true; ; first = false {
		for i := 0; i < len(ck.servers); i++ {
			srv := (ck.last_leader + i) % len(ck.servers)
			args := rpc.GetArgs{Key: key}
			var reply rpc.GetReply
			clerkDbgPrint(ck, "calling Get on server", srv, "with args", fmt.Sprintf("%+v", args))
			ok := ck.clnt.Call(ck.servers[srv], "KVServer.Get", &args, &reply)
			if !ok {
				clerkDbgPrint(ck, "Get on server", srv, "failed (not ok)")
				continue
			}
			clerkDbgPrint(ck, "Get on server", srv, "reply", fmt.Sprintf("%+v", reply))
			if reply.Err == rpc.OK {
				ck.last_leader = srv
				return reply.Value, reply.Version, rpc.OK
			} else if reply.Err == rpc.ErrWrongLeader {
				continue
			} else if reply.Err == rpc.ErrNoKey {
				ck.last_leader = srv
				if first {
					return "", 0, rpc.ErrNoKey
				}
				return "", 0, rpc.ErrMaybe
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	// You will have to modify this function.
	return "", 0, ""
}
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
  // 1. Define 'first' outside the loop
  first := true 
  
  for { 
    for i := 0; i < len(ck.servers); i++ {
      srv := (ck.last_leader + i) % len(ck.servers)
      args := rpc.PutArgs{Key: key, Value: value, Version: version}
      var reply rpc.PutReply
      
      ok := ck.clnt.Call(ck.servers[srv], "KVServer.Put", &args, &reply)
      
      if !ok {
        // 2. CRITICAL FIX:
        // If the RPC failed (timed out), the server MIGHT have processed it.
        // Therefore, any future attempt is NO LONGER the "first".
        first = false 
        continue
      }

      if reply.Err == rpc.OK {
        ck.last_leader = srv
        return rpc.OK
      } else if reply.Err == rpc.ErrVersion {
        ck.last_leader = srv
        if first {
          return rpc.ErrVersion // Definitely failed
        } else {
          return rpc.ErrMaybe   // Might have succeeded on a previous timeout
        }
      } else if reply.Err == rpc.ErrMaybe {
        return rpc.ErrMaybe
      } else if reply.Err == rpc.ErrNoKey {
				return rpc.ErrNoKey
			}
      
    }
    time.Sleep(20 * time.Millisecond)
    
    // 3. Ensure first is false for subsequent outer loops too
    first = false 
  }
  return ""
}