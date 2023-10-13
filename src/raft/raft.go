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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role int

const (
	Leader    = 0
	Candidate = 1
	Follower  = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role          Role
	currentTerm   int
	votedFor      int
	heartbeatChan chan byte
	logs          []LogEntry

	commitedIndex int
	lastApplied   int

	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Term    int
	Command any
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	term := args.Term
	reply.VoteGranted = false

	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentTerm := rf.currentTerm

	if term == currentTerm {
		if rf.votedFor == args.CandidateId || rf.votedFor == -1 {
			DPrintf("%v vote to %v", rf.me, args.CandidateId)
			reply.VoteGranted = true
		}
	} else if term > currentTerm {

		rf.currentTerm = term

		if args.LastLogTerm < rf.logs[len(rf.logs)-1].Term {
			reply.Term = rf.currentTerm
			return
		} else if args.LastLogTerm == rf.logs[len(rf.logs)-1].Term {
			if args.LastLogIndex < len(rf.logs)-1 {
				reply.Term = rf.currentTerm
				return
			}
		}
		DPrintf("%v force vote to %v", rf.me, args.CandidateId)

		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		// 无条件跟随该节点
		if rf.role != Follower {
			rf.role = Follower
			go rf.ticker()
		}

	} else {
		reply.Term = rf.currentTerm
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
	// DPrintf("%v sendRequestVote to %v ", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// my code
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIdx   int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// my code
func (rf *Raft) RequestHeartbeat(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("%v received heartbeat from %v self term %v, request term %v", rf.me, args.LeaderId, rf.currentTerm, args.Term)
	defer rf.mu.Unlock()

	switch rf.role {
	case Leader:
		if args.Term > rf.currentTerm {
			rf.role = Follower
			rf.currentTerm = args.Term
			go rf.ticker()
		}
	case Candidate:

		// 接收到其他节点的heartbeat
		rf.role = Follower
		rf.currentTerm = args.Term
		reply.Success = true
		go rf.ticker()

	case Follower:
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			reply.Success = true
		} else if args.Term == rf.currentTerm {
			reply.Term = rf.currentTerm
			reply.Success = true
		} else if args.Term < rf.currentTerm {
			//收到过期包处理？
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}
		// 只有Follower会通过接收到heartbeat来刷新定时器
		rf.heartbeatChan <- 0
	}
	// rf.currentTerm = args.Term

	// rf.heartbeat <- 0
}

func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if len(args.Entries) != 0 {

		switch rf.role {
		case Leader:
		case Candidate:
			rf.role = Follower
			rf.currentTerm = args.Term
			go rf.ticker()
			// 需要转至Follower处理过程
			fallthrough
		case Follower:
			if args.PrevLogIdx < len(rf.logs) && args.PrevLogTerm != rf.logs[args.PrevLogIdx].Term {
				rf.logs = rf.logs[:args.PrevLogIdx]
			} else if args.PrevLogIdx < len(rf.logs) && args.PrevLogTerm == rf.logs[args.PrevLogIdx].Term && args.PrevLogIdx+len(args.Entries) >= len(rf.logs) {

				rf.logs = append(rf.logs[:args.PrevLogIdx+1], args.Entries...)
				reply.Success = true
				DPrintf("[%v] replicate logs [%v], compilete self logs [%v]", rf.me, args, rf.logs)
				// DPrintf("[%v] replicate logs", rf.me)
			}
		}
	} else {

		switch rf.role {
		case Leader:
		case Candidate:
			rf.role = Follower
			rf.currentTerm = args.Term
			go rf.ticker()
			// 需要转至Follower处理过程
			fallthrough
		case Follower:
			// 防止通畅度但不同日志的follower进行commit
			if args.PrevLogIdx < len(rf.logs) && args.PrevLogTerm == rf.logs[args.PrevLogIdx].Term {

				if args.LeaderCommit > rf.commitedIndex {
					rf.commitedIndex = min(args.LeaderCommit, len(rf.logs)-1)
				}

				for rf.lastApplied < rf.commitedIndex {

					rf.lastApplied++
					DPrintf("notify Follower [%v] commit [%v] self log %v commitedIndex %v", rf.me, rf.logs[rf.lastApplied], rf.logs, args.LeaderCommit)
					rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.logs[rf.lastApplied].Command, CommandIndex: rf.lastApplied}
					reply.Success = true
				}
			}

		}
	}

}

func (rf *Raft) sendHeartbeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestHeartbeat", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	return ok
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
	// index := -1
	// term := -1
	// isLeader := true

	// Your code here (2B).
	if rf.killed() {
		return -1, -1, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.logs)
	term := rf.currentTerm
	isLeader := rf.role == Leader
	if isLeader {
		rf.logs = append(rf.logs, LogEntry{rf.currentTerm, command})
		DPrintf("%v server start command [%v], index[%v]", rf.me, command, len(rf.logs)-1)
		go rf.mayAppendCommand(command, len(rf.logs)-1)
	}

	return index, term, isLeader
}

func (rf *Raft) mayAppendCommand(command interface{}, mayCommitIndex int) {

	success := 1
	// finished := 1
	cond := sync.NewCond(&rf.mu)
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}

		go func(server int) {
			for !rf.killed() {
				args := AppendEntriesArgs{}
				rf.mu.Lock()
				// 如果在发送请求时候发现自己身份转换了，需要取消该线程
				if rf.role != Leader {
					rf.mu.Unlock()
					return
				}
				// 如果发现日志中新增了条目，则该循环请求取消
				if mayCommitIndex < len(rf.logs)-1 {
					rf.mu.Unlock()
					return
				}
				args.PrevLogIdx = rf.nextIndex[server] - 1
				args.Term = rf.currentTerm
				args.PrevLogTerm = rf.logs[rf.nextIndex[server]-1].Term
				args.LeaderCommit = rf.commitedIndex
				args.LeaderId = rf.me
				args.Entries = rf.logs[rf.nextIndex[server] : mayCommitIndex+1]
				rf.mu.Unlock()
				reply := AppendEntriesReply{}

				ok := rf.sendAppendEntries(server, &args, &reply)
				if !ok {
					DPrintf("[%v] replicate command to %v error", rf.me, server)
				}
				rf.mu.Lock()
				if !reply.Success {
					if rf.nextIndex[server] > 1 {
						rf.nextIndex[server]--
					}

					rf.mu.Unlock()
					time.Sleep(time.Duration(10) * time.Millisecond)
					continue
				} else {
					success++
					rf.nextIndex[server] = mayCommitIndex + 1
					cond.Broadcast()
					rf.mu.Unlock()
					return
				}

			}

		}(idx)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	for success <= len(rf.peers)/2 {
		if rf.killed() {
			return
		}
		cond.Wait()
	}
	if mayCommitIndex < len(rf.logs)-1 {
		return
	}
	DPrintf("[%v] append command [%v] to majority success, index [%v]", rf.me, command, mayCommitIndex)
	// 防止先发起的请求由于网络原因延迟而后收到回复导致较短日志条目被确认
	rf.commitedIndex = max(mayCommitIndex, rf.commitedIndex)
	for rf.lastApplied < rf.commitedIndex {

		rf.lastApplied++
		DPrintf("Leader [%v] commit  commit [%v]", rf.me, rf.logs[rf.lastApplied])
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.logs[rf.lastApplied].Command, CommandIndex: rf.lastApplied}
	}

	// 让follower同步commit
	for idx, _ := range rf.peers {
		if idx == rf.me {
			continue
		}

		go func(server int) {
			for !rf.killed() {
				args := AppendEntriesArgs{}
				rf.mu.Lock()

				// 如果在发送请求时候发现自己身份转换了，需要取消该线程
				if rf.role != Leader {
					rf.mu.Unlock()
					return
				}
				// 如果后发送的日志同步请求先完成，则该循环请求取消
				if mayCommitIndex < len(rf.logs)-1 {
					rf.mu.Unlock()
					return
				}
				args.PrevLogIdx = rf.nextIndex[server] - 1
				args.Term = rf.currentTerm
				args.PrevLogTerm = rf.logs[rf.nextIndex[server]-1].Term
				args.LeaderCommit = rf.commitedIndex
				args.LeaderId = rf.me
				args.Entries = make([]LogEntry, 0)
				rf.mu.Unlock()
				reply := AppendEntriesReply{}

				ok := rf.sendAppendEntries(server, &args, &reply)
				if !ok {
					DPrintf("[%v] notify commit to [%v] error", rf.me, server)

				}

				if reply.Success {
					return
				}
				time.Sleep(time.Duration(10) * time.Millisecond)
			}

			// rf.mu.Lock()
			// if !reply.Success {
			// 	rf.nextIndex[server]--
			// 	rf.mu.Unlock()
			// 	continue
			// } else {
			// 	success++
			// 	cond.Broadcast()
			// 	rf.mu.Unlock()
			// 	return
			// }

		}(idx)
	}

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

// 选举超时时间
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		// DPrintf("%v server ticker in term %v", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350F
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)

		ms := 400 + (rand.Int63() % 200)
		timeout := make(chan byte, 1)
		go func() {
			time.Sleep(time.Duration(ms) * time.Millisecond)
			timeout <- 0
		}()
		// rf.mu.Lock()
		// defer rf.mu.Unlock()
		select {
		case <-rf.heartbeatChan:
			// DPrintf("%v received heartbeat in term %v", rf.me, rf.currentTerm)
		case <-timeout:

			rf.mu.Lock()
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
			DPrintf("%v not received heartbeat in term %v", rf.me, rf.currentTerm)
			rf.role = Candidate
			rf.mu.Unlock()
			go rf.voteTimeoutTicker()
			return
		}

	}
}

func (rf *Raft) heartbeatTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != Leader {

			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		ms := 50 + (rand.Int63() % 200)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
		timeout := make(chan byte, 1)
		go func() {
			time.Sleep(time.Duration(ms) * time.Millisecond)
			timeout <- 0
		}()
		heartbeatComplete := make(chan bool, 1)
		go rf.heartbeat(heartbeatComplete)
		select {
		case res := <-heartbeatComplete:
			// 减少心跳发送频率
			<-timeout
			if !res {
				rf.mu.Lock()
				DPrintf("%v not be thinked as leader %v", rf.me, rf.currentTerm)
				rf.role = Follower
				go rf.ticker()
				rf.mu.Unlock()
			}
		case <-timeout:
			rf.mu.Lock()
			DPrintf("%v not get most heartbeat responce in term %v", rf.me, rf.currentTerm)
			rf.role = Follower
			go rf.ticker()
			rf.mu.Unlock()
		}

	}

}

// 防止VoteRequest超时导致Candidate一直处于选举状态中
func (rf *Raft) voteTimeoutTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != Candidate {

			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		// DPrintf("%v vote ticker", rf.me, rf.currentTerm)
		ms := 400 + (rand.Int63() % 200)
		timeout := make(chan int, 1)
		rf.mu.Lock()
		currentTerm := rf.currentTerm
		rf.mu.Unlock()
		go func() {
			time.Sleep(time.Duration(ms) * time.Millisecond)
			timeout <- currentTerm
		}()
		voteout := make(chan bool, 1)
		go rf.leaderElection(voteout)

		select {
		case res := <-voteout:

			rf.mu.Lock()
			if res {
				// DPrintf("not timeout")
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		case term := <-timeout:
			DPrintf("%v election timeout in term %v", rf.me, term)
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeatChan = make(chan byte, 1)
	rf.logs = append(rf.logs, LogEntry{Term: -1, Command: "empty"})
	rf.commitedIndex = 0
	rf.lastApplied = 0

	DPrintf("%v server initial", rf.me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) leaderElection(res chan bool) {

	rf.mu.Lock()

	if rf.killed() || rf.role != Candidate {
		res <- false
		rf.mu.Unlock()
		return
	}

	rf.votedFor = rf.me
	rf.currentTerm++
	vote := 1
	finished := 1
	DPrintf("%v start election in term %v", rf.me, rf.currentTerm)
	cond := sync.NewCond(&rf.mu)
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.logs) - 1
	args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
	rf.mu.Unlock()
	for idx, _ := range rf.peers {
		go func(server int) {
			reply := RequestVoteReply{}
			if server == rf.me {
				return
			}
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				DPrintf("[%v] sendRequestVote to %v error in term [%v]", rf.me, server, args.Term)

			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.VoteGranted {
				vote++

			}
			finished++
			cond.Broadcast()
		}(idx)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// res := make(chan bool, 1)

	for vote <= len(rf.peers)/2 && finished < len(rf.peers) {
		DPrintf("%v leaderElection track in term %v", rf.me, rf.currentTerm)

		cond.Wait()
	}

	if rf.killed() || rf.role != Candidate {
		res <- false
		// 如果有其他节点竞选成功并且发送了心跳导致自己变成follower，则不需要再等
		if rf.role != Candidate {
			DPrintf("%v other win in term %v", rf.me, rf.currentTerm)
		}

		return
	}

	if vote <= len(rf.peers)/2 {
		res <- false
		DPrintf("%v election fail in term %v", rf.me, rf.currentTerm)
		// return res
	} else {
		res <- true
		rf.role = Leader
		rf.nextIndex = make([]int, len(rf.peers))
		for idx := range rf.nextIndex {
			rf.nextIndex[idx] = len(rf.logs)
		}
		rf.matchIndex = make([]int, len(rf.peers))
		DPrintf("%v election success in term %v", rf.me, rf.currentTerm)
		go rf.heartbeatTicker()
		// return res
	}
}

func (rf *Raft) heartbeat(res chan bool) {

	tickerStart := false
	success := 1
	finished := 1
	cond := sync.NewCond(&rf.mu)
	for idx, _ := range rf.peers {

		go func(server int) {

			// defer rf.mu.Unlock()
			if server == rf.me {
				return
			}
			rf.mu.Lock()
			// DPrintf("%v send heart beat to %v in term %v", rf.me, server, rf.currentTerm)
			args := AppendEntriesArgs{}
			args.LeaderId = rf.me
			args.Term = rf.currentTerm
			rf.mu.Unlock()
			reply := AppendEntriesReply{}
			ok := rf.sendHeartbeat(server, &args, &reply)
			if !ok {
				DPrintf("[%v] send heart beat to [%v] error in term[%v]", rf.me, server, args.Term)
				rf.mu.Lock()
				finished++
				rf.mu.Unlock()
				return
			} else {
				rf.mu.Lock()
				success++
				finished++

				// 如果follower的term号比自己高，则可能需要让位
				if !reply.Success {
					DPrintf("[%v] term [%v] is higher than self %v", server, reply.Term, rf.currentTerm)
					rf.role = Follower
					rf.currentTerm = reply.Term
					if !tickerStart {
						tickerStart = true
						go rf.ticker()
					}
				}
				rf.mu.Unlock()
			}
			cond.Broadcast()
		}(idx)

	}

	rf.mu.Lock()
	for success <= len(rf.peers)/2 && finished < len(rf.peers) {
		cond.Wait()
	}
	if success <= len(rf.peers)/2 {
		res <- false

	} else {
		res <- true

	}
	rf.mu.Unlock()
}
