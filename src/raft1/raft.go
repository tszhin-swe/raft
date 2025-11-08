package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type State int

const (
	Candidate State = iota
	Follower
	Leader
)

type Log struct {
	Term    int
	Content interface{}
}

func dbgprint(rf *Raft, str string) {
	stateStr := map[State]string{Follower: "Follower", Candidate: "Candidate", Leader: "Leader"}[rf.state]
	timeStr := time.Now().Format("15:04:05.000")
	fmt.Printf("[Node %d][%s] %s Term: %d [%s]\n", rf.me, stateStr, str, rf.currentTerm, timeStr)
}

func dbgprintstruct(rf *Raft, str string) {
	id := rf.me
	stateStr := map[State]string{Follower: "Follower", Candidate: "Candidate", Leader: "Leader"}[rf.state]
	// log := rf.log
	commitIdx := rf.commitIndex
	lastApplied := rf.lastApplied
	currentTerm := rf.currentTerm
	fmt.Printf("apply changes called [Node %d][%s] %s Term: %d CommitIdx: %d LastApplied: %d Log: %+v\n", id, stateStr, str, currentTerm, commitIdx, lastApplied, "")
}

type RaftPersistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []Log
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex            // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd   // RPC end points of all peers
	persister *tester.Persister     // Object to hold this peer's persisted state
	me        int                   // this peer's index into peers[]
	applyCh   chan raftapi.ApplyMsg // channel to send committed log entries to the service/tester
	dead      int32                 // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state State
	// persistent
	currentTerm int
	votedFor    int // idx
	log         []Log

	// volatile state
	commitIndex int
	lastApplied int

	// leader only
	nextIndex  []int
	matchIndex []int

	// last_recv
	timestamp time.Time
	timeout   time.Duration
}

func (rf *Raft) GetRaftStateForTest() string {
	stateStr := map[State]string{Follower: "Follower", Candidate: "Candidate", Leader: "Leader"}[rf.state]
	return fmt.Sprintf("dump Node %d State: %s Term: %d VotedFor: %d CommitIdx: %d LastApplied: %d Log: %+v", rf.me, stateStr, rf.currentTerm, rf.votedFor, rf.commitIndex, rf.lastApplied, rf.log)
}

func (rf *Raft) OnElected() {
	rf.state = Leader
	// Initialize nextIndex to the index after the last log entry for each peer
	rf.nextIndex = make([]int, len(rf.peers))
	next := len(rf.log)
	for i := range rf.nextIndex {
		rf.nextIndex[i] = next
	}
	rf.matchIndex = make([]int, len(rf.peers))
	match := 0
	for i := range rf.nextIndex {
		rf.matchIndex[i] = match
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) UpdateTerm(newTerm int) {
	rf.state = Follower
	if newTerm > rf.currentTerm {
		rf.currentTerm = newTerm
		rf.votedFor = -1
		rf.state = Follower
		dbgprint(rf, "demoted to follower")
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	state := RaftPersistentState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(state)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(data) < 1 { // bootstrap without any state?
		rf.votedFor = -1
		rf.currentTerm = 0
		rf.log = append(rf.log, Log{Term: 0, Content: ""})
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var state RaftPersistentState
	if d.Decode(&state) != nil {
		tester.AnnotateCheckerFailureBeforeExit("failed to read persist", "decode error")
	}
	rf.currentTerm = state.CurrentTerm
	rf.votedFor = state.VotedFor
	rf.log = state.Log
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type AppendEntriesArgs struct {
	Term            int
	LeaderId        int
	PrevLogIdx      int
	PrevLogTerm     int
	Entries         []Log
	LeaderCommitIdx int
}

type AppendEntriesArgReply struct {
	Term    int
	Success bool
	ConflictIdx int
	ConflictTerm int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesArgReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persist()
	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	rf.UpdateTerm(args.Term)
	rf.timestamp = time.Now()
	reply.Term = rf.currentTerm

	if (args.PrevLogIdx >= len(rf.log)) {
		reply.ConflictIdx = len(rf.log)
		reply.ConflictTerm = -1
		return
	}
	
	if (rf.log[args.PrevLogIdx].Term != args.PrevLogTerm) {
		reply.ConflictTerm = rf.log[args.PrevLogIdx].Term
		// find first index of that term
		for i := 0; i <= args.PrevLogIdx; i++ {
			if rf.log[i].Term == reply.ConflictTerm {
				reply.ConflictIdx = i
				break
			}
		}
		return
	}

	dbgprint(rf, "received appendEntry ")

	rf.log = rf.log[:args.PrevLogIdx+1]
	rf.log = append(rf.log, args.Entries...)
	if args.LeaderCommitIdx > rf.commitIndex {
		rf.commitIndex = min(len(rf.log)-1, args.LeaderCommitIdx)
		rf.ApplyChanges()
	}
	reply.Success = true
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateId int
	LastLogIdx  int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	dbgprint(rf, "Received Request Vote")
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.persist()
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	rf.UpdateTerm(args.Term)
	reply.Term = rf.currentTerm

	if rf.votedFor != -1 && (rf.votedFor != args.CandidateId) {
		return
	}

	isCandUpToDate := (args.LastLogTerm > rf.log[len(rf.log)-1].Term) || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && (args.LastLogIdx >= len(rf.log)-1))
	if isCandUpToDate {
		dbgprint(rf, "vote granted")
		rf.timestamp = time.Now()
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesArgReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) ApplyChanges() {
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Content,
			CommandIndex: i,
		}
		rf.applyCh <- msg
		dbgprint(rf, "applied changes"+fmt.Sprint(msg))

	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) CheckCommit() {
	copyArr := make([]int, len(rf.matchIndex))
	copy(copyArr, rf.matchIndex)
	// Sort the copy
	sort.Ints(copyArr)
	if copyArr[len(rf.peers)/2] > rf.commitIndex && rf.log[copyArr[len(rf.peers)/2]].Term == rf.currentTerm {
		dbgprint(rf, "commit index updated to "+fmt.Sprint(copyArr[len(rf.peers)/2]))
		new_commit_idx := max(rf.commitIndex, copyArr[len(rf.peers)/2])
		rf.commitIndex = new_commit_idx
		rf.ApplyChanges()
	}
}

func (rf *Raft) ReplicateEachPeer(peer int, nextIndex int, matchIndex int) bool {
	rf.mu.Lock()
	last_log_idx := len(rf.log) - 1
	if !(last_log_idx >= nextIndex) {
		return false
	}
	// make a copy
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
		// PrevLogIdx:      nextIndex - 1,
		// PrevLogTerm:     rf.log[nextIndex - 1].Term,
		// Entries:         rf.log[nextIndex:],
		LeaderCommitIdx: rf.commitIndex,
	}
	old_term := rf.currentTerm

	rf.mu.Unlock()
	i := nextIndex
	for  i > 0 {
		reply := AppendEntriesArgReply{}
		dbgprint(rf, "sending to peer "+fmt.Sprint(peer))
		args.PrevLogIdx = i - 1
		args.PrevLogTerm = rf.log[i-1].Term
		args.Entries = rf.log[i:]

		for !rf.sendAppendEntries(peer, &args, &reply) { // need retry
			time.Sleep(10 * time.Millisecond)
			rf.mu.Lock()
			if rf.state != Leader {
				reply.Success = false;
				rf.mu.Unlock()
				return false
			}
			rf.mu.Unlock()

		}
		// can return when replicated successful, or term changed, or demoted
		can_return := func() bool {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.persist()

			if old_term < rf.currentTerm {
				return true
			}
			if reply.Term > rf.currentTerm {
				rf.UpdateTerm(reply.Term) // todo
				return true
			}

			if reply.Success {
				rf.matchIndex[peer] = args.PrevLogIdx + len(args.Entries)
				rf.nextIndex[peer] = rf.matchIndex[peer]	+ 1
				fmt.Printf("peer %d replicated up to %d\n", peer, nextIndex)
				rf.CheckCommit()
				return true
			} else {
				if reply.ConflictTerm != -1 {
					for j := 0; j < len(rf.log); j++ {
						if rf.log[j].Term > reply.ConflictTerm {
							i = j
							break
						}
					}
				} 
				i = reply.ConflictIdx
			}
			rf.nextIndex[peer] = i
			return false
		}()
		if can_return {
			return true
		}
	}
	return true

}

// func (rf *Raft) ReplicateProc(log_idx int) {
// 	for peer := range len(rf.peers) {
// 		go rf.ReplicateEachPeer(peer, rf.nextIndex[peer], rf.matchIndex[peer])
// 	}
// }

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	dbgprint(rf, "===Start called"+fmt.Sprint(command)+"===")

	rf.log = append(rf.log, Log{Term: rf.currentTerm, Content: command})
	index := len(rf.log) - 1
	term := rf.currentTerm

	// set my own nextIndex and matchIndex
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	for peer := range len(rf.peers) {
		if peer == rf.me {
			continue
		}
		go rf.ReplicateEachPeer(peer, rf.nextIndex[peer], rf.matchIndex[peer])
	}
	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	rf.timestamp = time.Now()
	rf.currentTerm++
	term := rf.currentTerm
	rf.votedFor = -1

	rf.state = Candidate
	currVotes := 1
	dbgprint(rf, "start election")

	// Prepare RequestVoteArgs
	lastLogIdx := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIdx].Term

	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		LastLogIdx:  lastLogIdx,
		LastLogTerm: lastLogTerm,
	}

	rf.mu.Unlock()
	for peer := range len(rf.peers) {
		if peer == rf.me {
			continue
		}
		go func(peer int, term int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(peer, args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				rf.persist()

				if term < rf.currentTerm {
					return
				}
				if reply.Term > rf.currentTerm {
					rf.UpdateTerm(reply.Term)
					return
				}
				if reply.VoteGranted {
					currVotes++
					if currVotes > len(rf.peers)/2 {
						rf.OnElected()
					}
				}
			}
		}(peer, term)

	}

}

func (rf *Raft) checkElectionTimeout() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.state != Leader && time.Since(rf.timestamp) >= rf.timeout {
			rf.startElection() // inside will release mu
		} else {
			rf.mu.Unlock()
		}

		// Sleep for a short duration to avoid busy-waiting; can be 10-20ms for responsive election checks.
		time.Sleep(10 * time.Millisecond)

	}

}

func (rf *Raft) sendHeartBeatIfLeader() {
	print("hi")
	for !rf.killed() {
		rf.mu.Lock()
		term := rf.currentTerm
		if rf.state == Leader {
			args := &AppendEntriesArgs{
				Term:            rf.currentTerm,
				LeaderId:        rf.me,
				PrevLogIdx:      len(rf.log) - 1,
				PrevLogTerm:     rf.log[len(rf.log)-1].Term,
				Entries:         []Log{},
				LeaderCommitIdx: rf.commitIndex,
			}
			rf.mu.Unlock()
			for peer := range len(rf.peers) {
				if peer == rf.me {
					continue
				}
				go func(peer int, term int) {
					reply := AppendEntriesArgReply{}
					if rf.sendAppendEntries(peer, args, &reply) {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if term < rf.currentTerm {
							return
						}
						if reply.Term > rf.currentTerm {
							rf.UpdateTerm(reply.Term)
							return
						}
					}
				}(peer, term)
			}
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	labgob.Register(Log{})
	labgob.Register(RaftPersistentState{})

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.applyCh = applyCh
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.lastApplied = 0
	rf.state = Follower
	rf.commitIndex = 0

	// rf.currentTerm = 0
	// rf.votedFor = -1
	// rf.log = append(rf.log, Log{Term: 0, Content: ""})

	rf.timestamp = time.Now()
	rf.timeout = time.Duration(300+(300/(len(rf.peers)))*rf.me) * time.Millisecond

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.checkElectionTimeout()
	go rf.sendHeartBeatIfLeader()

	return rf
}
