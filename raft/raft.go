package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogElement struct {
	Term    int
	Command interface{}
	Index   int
}

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	votedFor         int
	numVotesReceived int
	state            int
	currentTerm      int
	log              []LogElement
	commitIdx        int
	nextIdx          []int // only for leaders
	matchIdx         []int // only for leaders
	appliedLast      int
	cApplyMsg        chan ApplyMsg
	cWinElection     chan struct{}
	cHeartbeat       chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, (rf.state == LEADER)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// NOTE: only call when holding lock
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	// w := new(bytes.Buffer)
	DPrintf("(persist) %d: Persisting state", rf.me)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// rf.mu.Lock()
	// rf.mu.Unlock()
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.log) != nil {
		DPrintf("(persist) %d: Error encoding state", rf.me)
		return
	}
	rf.persister.SaveRaftState(w.Bytes())
}

// restore previously persisted state.
// NOTE: only call when holding lock
func (rf *Raft) readPersist(data []byte) {
	DPrintf("(readPersist) %d: Reading persisted state", rf.me)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// decodes in place?
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	if d.Decode(&rf.currentTerm) != nil || d.Decode(&rf.votedFor) != nil || d.Decode(&rf.log) != nil {
		DPrintf("(readPersist) %d: Error decoding state", rf.me)
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	IDcand      int
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		DPrintf("(RequestVote) %d: Rejecting vote request from %d because my term is greater", rf.me, args.IDcand)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if rf.currentTerm < args.Term {
		DPrintf("(RequestVote) %d: Updating term to %d", rf.me, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1 // invalidate previous vote in new period
		rf.state = FOLLOWER
	}
	reply.Term = args.Term

	// TODO: check this logic
	if (args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIdx >= rf.log[len(rf.log)-1].Index)) && (rf.votedFor == -1 || rf.votedFor == args.IDcand) {
		DPrintf("(RequestVote) %d: Granting vote to %d", rf.me, args.IDcand)
		reply.VoteGranted = true
		rf.votedFor = args.IDcand
		rf.cHeartbeat <- struct{}{} // workaround for the one() test erroring out b/c chosen leader at beginning is not leader at time of check?
	} else {
		DPrintf("(RequestVote) %d: Rejecting vote request from %d", rf.me, args.IDcand)
		reply.VoteGranted = false
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
	// dont lock on blocking io
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if rf.currentTerm != args.Term || rf.state != CANDIDATE {
			DPrintf("(sendRequestVote) %d: is not a candidate or behind arg term", rf.me)
			return ok
		}
		if reply.Term > rf.currentTerm {
			DPrintf("(sendRequestVote) %d: Updating my term to %d", rf.me, reply.Term)
			rf.votedFor = -1
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			return ok
		}
		if reply.VoteGranted {
			DPrintf("(sendRequestVote) %d: Received vote from %d", rf.me, server)
			rf.numVotesReceived++
			if rf.numVotesReceived > len(rf.peers)/2 {
				DPrintf("(sendRequestVote) %d: Won election", rf.me)
				rf.persist()
				nextIdx := rf.log[len(rf.log)-1].Index + 1
				for i := range rf.peers {
					rf.nextIdx[i] = nextIdx
				}
				rf.state = LEADER
				rf.cWinElection <- struct{}{}
			}
		}
	}

	return ok
}

// Figure 2 of Raft paper
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIdx   int
	PrevLogTerm  int
	Entries      []LogElement
	LeaderCommit int
}

type AppendEntriesResults struct {
	Term       int
	Success    bool
	NextTryIdx int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesResults) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogIdx := rf.log[len(rf.log)-1].Index
	baseIdx := rf.log[0].Index
	if rf.currentTerm > args.Term {
		DPrintf("(AppendEntries) %d: Rejecting append entries from %d because my term is greater", rf.me, args.LeaderId)
		reply.Success = false
		reply.NextTryIdx = lastLogIdx + 1
		reply.Term = rf.currentTerm
		// rf.persist()
		return
	}
	if rf.currentTerm < args.Term {
		DPrintf("(AppendEntries) %d: Updating term to %d", rf.me, args.Term)
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
	}
	reply.Term = args.Term
	rf.cHeartbeat <- struct{}{}
	if args.PrevLogIdx > lastLogIdx {
		DPrintf("(AppendEntries) %d: Rejecting append entries from %d because args.PrevLogIdx > lastLogIdx", rf.me, args.LeaderId)
		reply.NextTryIdx = lastLogIdx + 1
		// rf.persist()
		return
	}
	if args.PrevLogIdx >= baseIdx && rf.log[args.PrevLogIdx-baseIdx].Term != args.PrevLogTerm {
		term := rf.log[args.PrevLogIdx-baseIdx].Term
		for i := args.PrevLogIdx - 1; i >= baseIdx; i-- {
			if rf.log[i-baseIdx].Term != term {
				reply.NextTryIdx = i + 1
				// rf.persist()
				return
			}
		}
	} else if args.PrevLogIdx > baseIdx-2 {
		rf.log = rf.log[:args.PrevLogIdx-baseIdx+1]
		rf.log = append(rf.log, args.Entries...)
		reply.NextTryIdx = len(args.Entries) + args.PrevLogIdx
		reply.Success = true
		if args.LeaderCommit > rf.commitIdx {
			rf.commitIdx = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
			DPrintf("(AppendEntries) %d: Committing logs up to %d", rf.me, rf.commitIdx)
			go func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				for i := rf.appliedLast + 1; i <= rf.commitIdx; i++ {
					msg := ApplyMsg{CommandValid: true, Command: rf.log[i-rf.log[0].Index].Command, CommandIndex: i}
					rf.cApplyMsg <- msg
				}
				rf.appliedLast = rf.commitIdx
			}()
		}
	}
	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, arg *AppendEntriesArgs, repl *AppendEntriesResults) {
	rf.mu.Lock()
	DPrintf("(sendAppendEntries) %d: Sending append entries to %d", rf.me, server)
	// deep copy to prevent that odd non-deterministic data race
	args := &AppendEntriesArgs{
		Term:         arg.Term,
		LeaderId:     arg.LeaderId,
		PrevLogIdx:   arg.PrevLogIdx,
		PrevLogTerm:  arg.PrevLogTerm,
		LeaderCommit: arg.LeaderCommit,
		Entries:      make([]LogElement, len(arg.Entries)),
	}
	copy(args.Entries, arg.Entries)

	reply := &AppendEntriesResults{
		Term:       repl.Term,
		Success:    repl.Success,
		NextTryIdx: repl.NextTryIdx,
	}
	rf.mu.Unlock()
	// dont block on io
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIdx := rf.log[0].Index
	if !ok || rf.currentTerm != args.Term || rf.state != LEADER {
		DPrintf("(sendAppendEntries) %d: Call failed, not a leader, or behind term", rf.me)
		return
	}
	if rf.currentTerm < reply.Term {
		DPrintf("(sendAppendEntries) %d: Other node has later term %d. Becoming follower ", rf.me, reply.Term)
		rf.votedFor = -1
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.persist() // NOTE: recent comment
		return
	}
	if reply.Success {
		if len(args.Entries) > 0 {
			rf.nextIdx[server] = args.Entries[len(args.Entries)-1].Index + 1
			rf.matchIdx[server] = rf.nextIdx[server] - 1
		}
	} else {
		rf.nextIdx[server] = reply.NextTryIdx
	}
	for i := rf.log[len(rf.log)-1].Index; i > rf.commitIdx && rf.currentTerm == rf.log[i-baseIdx].Term; i-- {
		count := 1
		for j := range rf.peers {
			if rf.matchIdx[j] >= i && j != rf.me {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIdx = i
			go func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				for i := rf.appliedLast + 1; i <= rf.commitIdx; i++ {
					msg := ApplyMsg{CommandValid: true, Command: rf.log[i-rf.log[0].Index].Command, CommandIndex: i}
					rf.cApplyMsg <- msg
				}
				rf.appliedLast = rf.commitIdx
			}()
			break
		}
	}
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("(broadcastHeartbeat) %d: Broadcasting heartbeat", rf.me)
	baseIdx := rf.log[0].Index

	for i := range rf.peers {
		if rf.state == LEADER && i != rf.me {
			if rf.nextIdx[i] <= baseIdx {
				// snapshot? but not necessary for lab
				// continue
			} else {
				args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIdx: rf.nextIdx[i] - 1, LeaderCommit: rf.commitIdx}
				if args.PrevLogIdx >= baseIdx {
					args.PrevLogTerm = rf.log[args.PrevLogIdx-baseIdx].Term
				}
				if rf.nextIdx[i] <= rf.log[len(rf.log)-1].Index {
					args.Entries = rf.log[rf.nextIdx[i]-baseIdx:]
				}
				go rf.sendAppendEntries(i, &args, &AppendEntriesResults{})
			}
		}
	}
}

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
	index := -1
	term := -1
	isLeader := rf.state == LEADER
	// Your code here (3B).

	if isLeader {
		term = rf.currentTerm
		index = rf.log[len(rf.log)-1].Index + 1
		rf.log = append(rf.log, LogElement{Term: term, Command: command, Index: index})
		rf.persist()
	}

	return index, term, isLeader
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	time.Sleep(time.Duration(1100) * time.Millisecond)
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		rf.mu.Lock()
		switch rf.state {
		case FOLLOWER:
			DPrintf("(ticker) %d: FOLLOWER in term %d", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			select {
			case <-time.After(time.Duration(rand.Intn(200)+1100) * time.Millisecond):
				rf.mu.Lock()
				rf.state = CANDIDATE
				rf.persist()
				rf.mu.Unlock()
			case <-rf.cHeartbeat:
				rf.mu.Lock()
				DPrintf("(ticker) %d: Received heartbeat", rf.me)
				rf.mu.Unlock()
			}
		case CANDIDATE:
			rf.currentTerm++
			DPrintf("(ticker) %d: CANDIDATE in term %d", rf.me, rf.currentTerm)
			rf.votedFor = rf.me
			rf.numVotesReceived = 1
			rf.persist()
			rf.mu.Unlock()
			// TODO sent req vote here
			go func() {
				rf.mu.Lock()
				args := RequestVoteArgs{Term: rf.currentTerm, IDcand: rf.me, LastLogIdx: rf.log[len(rf.log)-1].Index, LastLogTerm: rf.log[len(rf.log)-1].Term}
				rf.mu.Unlock()
				for i := range rf.peers {
					if i != rf.me {
						go func(i int) {
							rf.sendRequestVote(i, &args, &RequestVoteReply{})
						}(i)
					}
				}
			}()
			select {
			case <-rf.cWinElection:
			case <-time.After(time.Duration(rand.Intn(200)+600) * time.Millisecond):
				rf.mu.Lock()
				rf.state = FOLLOWER
				rf.mu.Unlock()
			case <-rf.cHeartbeat:
				rf.mu.Lock()
				rf.state = FOLLOWER
				DPrintf("(ticker) %d: Received heartbeat", rf.me)
				rf.mu.Unlock()

			}
		case LEADER:
			DPrintf("(ticker) %d: LEADER in term %d", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			go rf.broadcastHeartbeat()
			rf.mu.Lock()
			rf.persist()
			rf.mu.Unlock()
			time.Sleep(100 * time.Millisecond) // limited to 10 heartbeats per second

		}

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
	persister *Persister, applyCh chan ApplyMsg,
) *Raft {
	DPrintf("(Make) %d: Initializing a new Raft server", me)
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.numVotesReceived = 0
	// rf.votedFor = -1
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.commitIdx = 0
	rf.appliedLast = 0
	rf.log = append(rf.log, LogElement{Term: 0, Command: nil, Index: 0})
	rf.nextIdx = make([]int, len(rf.peers))
	rf.matchIdx = make([]int, len(rf.peers))

	rf.cApplyMsg = applyCh
	rf.cWinElection = make(chan struct{}, 1000)
	rf.cHeartbeat = make(chan struct{}, 1000)

	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	rf.votedFor = -1 // prevents gob warning
	rf.mu.Unlock()

	// start ticker goroutine to start elections
	DPrintf("(Make) %d: Starting ticker for server", me)
	go rf.ticker()

	return rf
}
