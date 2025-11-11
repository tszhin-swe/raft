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
	fmt.Printf("[Node %d][%s] %s Term: %d  [%s]\n", rf.me, stateStr, str, rf.currentTerm, timeStr)
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
	CurrentTerm      int
	VotedFor         int
	Log              []Log
	LastIncludedIdx  int
	LastIncludedTerm int
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
	// presisted snapshot
	lastIncludedIdx  int
	lastIncludedTerm int
	snapshot         []byte

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

func (rf *Raft) GetLastLogIndexAndTerm() (int, int) {
	if len(rf.log) == 0 {
		return rf.lastIncludedIdx, rf.lastIncludedTerm
	}
	return rf.lastIncludedIdx + len(rf.log), rf.log[len(rf.log)-1].Term
}

func (rf *Raft) GetLogAtIdx(idx int) Log {
	return rf.log[(idx - rf.lastIncludedIdx - 1)]
}

func (rf *Raft) GetTermAtIdx(idx int) int {
	if idx <= rf.lastIncludedIdx {
		return rf.lastIncludedTerm
	}
	return rf.GetLogAtIdx(idx).Term
}

func (rf *Raft) GetLogIdxFromAbsoluteIdx(idx int) int {
	return idx - rf.lastIncludedIdx - 1
}

func (rf *Raft) GetRaftStateForTest() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	stateStr := map[State]string{Follower: "Follower", Candidate: "Candidate", Leader: "Leader"}[rf.state]
	return fmt.Sprintf("dump Node %d State: %s Term: %d VotedFor: %d CommitIdx: %d LastApplied: %d LastIncludedIdx : %d Log: %+v", rf.me, stateStr, rf.currentTerm, rf.votedFor, rf.commitIndex, rf.lastApplied, rf.lastIncludedIdx ,rf.log)
}

func (rf *Raft) OnElected() {
	rf.state = Leader
	// Initialize nextIndex to the index after the last log entry for each peer
	rf.nextIndex = make([]int, len(rf.peers))
	last, _ := rf.GetLastLogIndexAndTerm()
	next := last + 1
	for i := range rf.nextIndex {
		rf.nextIndex[i] = next
	}
	rf.matchIndex = make([]int, len(rf.peers))
	match := 0
	for i := range rf.matchIndex {
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
	rf.persist()

}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	state := RaftPersistentState{
		CurrentTerm:      rf.currentTerm,
		VotedFor:         rf.votedFor,
		Log:              rf.log,
		LastIncludedIdx:  rf.lastIncludedIdx,
		LastIncludedTerm: rf.lastIncludedTerm,
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(state)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(data) < 1 { // bootstrap without any state?
		rf.votedFor = -1
		rf.currentTerm = 0
		rf.lastApplied = 0
		rf.lastIncludedIdx = 0
		rf.lastIncludedTerm = 0
		rf.snapshot = nil
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var state RaftPersistentState
	if d.Decode(&state) != nil {
		tester.AnnotateCheckerFailureBeforeExit("failed to read persist", "decode error")
	}
	rf.snapshot = rf.persister.ReadSnapshot()
	rf.currentTerm = state.CurrentTerm
	rf.votedFor = state.VotedFor
	rf.log = state.Log
	rf.lastIncludedIdx = state.LastIncludedIdx
	rf.lastIncludedTerm = state.LastIncludedTerm
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIdx {
		return
	}
	if index > rf.commitIndex {
		return
	}
	old_last_included_idx := rf.lastIncludedIdx
	rf.lastIncludedTerm = rf.log[rf.GetLogIdxFromAbsoluteIdx(index)].Term
	rf.lastIncludedIdx = index

	old_len := len(rf.log)
	rf.log = rf.log[index - old_last_included_idx:]
	new_len := len(rf.log)
	rf.snapshot = snapshot
	dbgprint(rf,"snapshot called for index" + fmt.Sprint(index) +" "+ fmt.Sprint(rf.lastIncludedIdx)+ " " + fmt.Sprint(rf.log) + " old len " + fmt.Sprint(old_len) + " new len " + fmt.Sprint(new_len))
	rf.persist()
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
	Term         int
	Success      bool
	ConflictIdx  int
	ConflictTerm int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesArgReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	dbgprint(rf, "Received Append Entries from peer "+fmt.Sprint(args.LeaderId))
	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	rf.UpdateTerm(args.Term)
	rf.timestamp = time.Now()
	reply.Term = rf.currentTerm

	last_log_idx, _ := rf.GetLastLogIndexAndTerm()

	if args.PrevLogIdx > last_log_idx {
		reply.ConflictIdx = last_log_idx + 1
		reply.ConflictTerm = -1
		return
	}

	// todo: figure this out
	// what if log index is smaller than last included idx?
	// we have commited, maybe we can return success?
	if args.PrevLogIdx < rf.lastIncludedIdx {
		reply.Success = true
		return
	}

	if rf.GetTermAtIdx(args.PrevLogIdx) != args.PrevLogTerm {
		reply.ConflictTerm = rf.GetTermAtIdx(args.PrevLogIdx)
		// find first index of that term
		for i := 0; i < len(rf.log); i++ {
			if rf.log[i].Term == reply.ConflictTerm {
				reply.ConflictIdx = rf.lastIncludedIdx + i + 1
				break
			}
		}
		return
	}

	dbgprint(rf, "received appendEntry from peer "+fmt.Sprint(args.LeaderId) + fmt.Sprint(args.Entries))

	rf.log = rf.log[:rf.GetLogIdxFromAbsoluteIdx(args.PrevLogIdx+1)]
	rf.log = append(rf.log, args.Entries...)
	rf.persist()
	if args.LeaderCommitIdx > rf.commitIndex {
		last_idx, _ := rf.GetLastLogIndexAndTerm()
		rf.commitIndex = min(last_idx, args.LeaderCommitIdx)
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
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	dbgprint(rf, "Received Request Vote")

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

	last_log_idx, last_term := rf.GetLastLogIndexAndTerm()
	isCandUpToDate := (args.LastLogTerm > last_term) || (args.LastLogTerm == last_term && (args.LastLogIdx >= last_log_idx))
	if isCandUpToDate {
		// dbgprint(rf, "vote granted")
		rf.timestamp = time.Now()
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
	}

}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludedIdx  int
	LastIncludedTerm int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	dbgprint(rf, "InstallSnapshot called")
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.UpdateTerm(args.Term)
	}
	reply.Term = rf.currentTerm

	// we have made a snapshot already
	if args.LastIncludedIdx <= rf.lastIncludedIdx {
		return
	}

	dbgprint(rf, "installing snapshot up to idx "+fmt.Sprint(args.LastIncludedIdx))
	snapshot := args.Data
	rf.snapshot = snapshot
	if rf.GetLogIdxFromAbsoluteIdx(args.LastIncludedIdx) < len(rf.log) && rf.GetTermAtIdx(args.LastIncludedIdx) == args.LastIncludedTerm {
		rf.log = rf.log[rf.GetLogIdxFromAbsoluteIdx(args.LastIncludedIdx+1):]
		rf.lastIncludedIdx = args.LastIncludedIdx
		rf.persist()
		return
	}
	rf.log = []Log{}
	rf.persist()
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) ApplyChanges() {
	commitIdx := rf.commitIndex
	for i := rf.lastApplied + 1; i <= commitIdx; i++ {
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.GetLogAtIdx(i).Content,
			CommandIndex: i,
		}
		rf.mu.Unlock()
		rf.applyCh <- msg
		rf.mu.Lock()
		dbgprint(rf, "applied changes"+fmt.Sprint(msg))
	}
	rf.lastApplied = commitIdx
}

func (rf *Raft) CheckCommit() {
	copyArr := make([]int, len(rf.matchIndex))
	copy(copyArr, rf.matchIndex)
	// Sort the copy
	sort.Ints(copyArr)
	if rf.GetLogIdxFromAbsoluteIdx(copyArr[len(rf.peers)/2]) >= len(rf.log) {
		print("sus3")
		return
	}
	if copyArr[len(rf.peers)/2] > rf.commitIndex && rf.GetTermAtIdx(copyArr[len(rf.peers)/2]) == rf.currentTerm {
		dbgprint(rf, "commit index updated to "+fmt.Sprint(copyArr[len(rf.peers)/2]))
		new_commit_idx := max(rf.commitIndex, copyArr[len(rf.peers)/2])
		rf.commitIndex = new_commit_idx
		rf.ApplyChanges()
	}
}

// when we call this, guarantee to succeed except if term outdated.
func (rf *Raft) SendSnapshot(peer int, args *InstallSnapshotArgs) {
	// args := InstallSnapshotArgs{
	// 	Term:             rf.currentTerm,
	// 	LeaderId:         rf.me,
	// 	LastIncludedIdx:  rf.lastIncludedIdx,
	// 	LastIncludedTerm: rf.lastIncludedTerm,
	// 	Data:             rf.snapshot,
	// }
	reply := InstallSnapshotReply{}

	ok := rf.sendInstallSnapshot(peer, args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.UpdateTerm(reply.Term)
		return
	}
	rf.nextIndex[peer] = args.LastIncludedIdx + 1
	rf.matchIndex[peer] = args.LastIncludedIdx
}

func (rf *Raft) ReplicateEachPeer(peer int, nextIndex int, matchIndex int, args *AppendEntriesArgs) bool {
	rf.mu.Lock()

	if rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return false
	}

	old_term := rf.currentTerm
	i := nextIndex

	rf.mu.Unlock()

	reply := AppendEntriesArgReply{}
	ok := rf.sendAppendEntries(peer, args, &reply)

	if !ok {
		dbgprint(rf, "no response from" + fmt.Sprint(peer))
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if old_term < rf.currentTerm {
		return true
	}
	if reply.Term > rf.currentTerm {
		rf.UpdateTerm(reply.Term) // todo
		return true
	}

	if reply.Success {
		rf.matchIndex[peer] = args.PrevLogIdx + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		fmt.Printf("peer %d replicated up to %d\n", peer, rf.matchIndex[peer])
		rf.CheckCommit()
		return true
	} else {
		dbgprint(rf, "conflict from " + fmt.Sprint(peer) + " " + fmt.Sprint(reply.ConflictIdx)+ " "+ fmt.Sprint(reply.ConflictTerm) + "setting nextIndex from " + fmt.Sprint(rf.nextIndex[peer]))
		if reply.ConflictTerm != -1 {
			found := false
			for j := len(rf.log) - 1; j >= 0; j-- {
				if rf.log[j].Term == reply.ConflictTerm {
					i = j + rf.lastIncludedIdx + 1 // after last entry with that term
					found = true
					break
				}
			}
			if !found {
				i = reply.ConflictIdx // term not found
			}
		} else {
			i = reply.ConflictIdx
		}
	}
	rf.nextIndex[peer] = i
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
	rf.persist()
	index,_ := rf.GetLastLogIndexAndTerm()
	term := rf.currentTerm

	// // set my own nextIndex and matchIndex
	// rf.nextIndex[rf.me] = index + 1
	// rf.matchIndex[rf.me] = index
	// for peer := range len(rf.peers) {
	// 	if peer == rf.me {
	// 		continue
	// 	}
	// 	go rf.ReplicateEachPeer(peer, rf.nextIndex[peer], rf.matchIndex[peer])
	// }
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
	lastLogIdx, lastLogTerm := rf.GetLastLogIndexAndTerm()

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
	for !rf.killed() {
		rf.mu.Lock()
		index, _ := rf.GetLastLogIndexAndTerm()
		if rf.state == Leader {
			// set my own nextIndex and matchIndex
			rf.nextIndex[rf.me] = index + 1
			rf.matchIndex[rf.me] = index
			for peer := range len(rf.peers) {
				if peer == rf.me {
					continue
				}
				next_idx_for_peer := rf.nextIndex[peer]
				if next_idx_for_peer > rf.lastIncludedIdx {
					args := &AppendEntriesArgs{
						Term:            rf.currentTerm,
						LeaderId:        rf.me,
						LeaderCommitIdx: rf.commitIndex,
						Entries:         rf.log[rf.GetLogIdxFromAbsoluteIdx(next_idx_for_peer):],
						PrevLogIdx:      next_idx_for_peer - 1,
						PrevLogTerm:     rf.GetTermAtIdx(next_idx_for_peer - 1),
					}
					go rf.ReplicateEachPeer(peer, rf.nextIndex[peer], rf.matchIndex[peer], args)
				} else {
					print("sus")
					args := &InstallSnapshotArgs{
						Term:             rf.currentTerm,
						LeaderId:         rf.me,
						LastIncludedIdx:  rf.lastIncludedIdx,
						LastIncludedTerm: rf.lastIncludedTerm,
						Data:             rf.snapshot,
					}
					go rf.SendSnapshot(peer, args)
				}
			}
		}
		rf.mu.Unlock()
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
