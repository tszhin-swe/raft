package rsm

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

func dbgprint(rsm *RSM, args ...interface{}) {
	timeStr := time.Now().Format("15:04:05.000")

	fmt.Print("[RSM] me=", rsm.me, " | ", timeStr)
	fmt.Println(args...)
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  string
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	submitCh     map[string]chan any
	map_mu       sync.Mutex
}

func (rsm *RSM) get_id() string {
	return uuid.New().String()
}

func (rsm *RSM) Read() {
	for msg := range rsm.applyCh {
		rsm.mu.Lock()
		dbgprint(rsm, "ReadGoroutine : received msg from applyCh", msg.Command.(Op))
		// if me := msg.Command.(Op).Me; me != rsm.me {
		// 	// no longer leader
		// 	for id, ch := range rsm.submitCh {
		// 		close(ch)
		// 		delete(rsm.submitCh, id)
		// 	}
		// }
		msg_id := msg.Command.(Op).Id
		outcome := rsm.sm.DoOp(msg.Command.(Op).Req)
		ch, ok := rsm.submitCh[msg_id]
		if !ok {
			rsm.mu.Unlock()
			continue
		}
		rsm.mu.Unlock()
		ch <- outcome
		// rsm.mu.Unlock()

	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		submitCh:     make(map[string]chan any),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.Read()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	start := time.Now()

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
	rsm.mu.Lock()
	id := rsm.get_id()
	op := Op{Me: rsm.me, Id: id, Req: req}
	defer func() {
		dbgprint(rsm, "Submit returned. Took", op, time.Since(start).Milliseconds(), "ms")
	}()
	dbgprint(rsm, "RSM Submit called with req: ", op, "\n")

	rsm.submitCh[id] = make(chan any,1)
	channel := rsm.submitCh[id]
	// defer delete(rsm.submitCh, id) // cleanup, todo
	_, oldTerm, isLeader := rsm.rf.Start(op)
	dbgprint(rsm, "RSM Submit rf has returned, took: ", op,time.Since(start).Milliseconds(), "\n")

	if !isLeader {
		rsm.mu.Unlock()
		dbgprint(rsm, "RSM Submit not leader: ", op, "\n")
		return rpc.ErrWrongLeader, nil
	}
	rsm.mu.Unlock()
	for {
		select {
		case action, ok := <-channel:
			if !ok {
				return rpc.ErrMaybe, nil
			}
			dbgprint(rsm, "RSM Submit received msg: ", op, "\n")
			return rpc.OK, action
		case <-time.After(10 * time.Millisecond):
			term, isLeader := rsm.rf.GetState()
			if term > oldTerm || !isLeader {
				return rpc.ErrMaybe, nil
			} else {
				continue
			}
		}
	}
}
