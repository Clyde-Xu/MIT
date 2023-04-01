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

	"bytes"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

type Role int

const (
	Leader Role = iota
	Candidate
	Follower
)

const (
	// ElectionTimeout =
	logging = false
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

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	role        Role
	applyCh     chan ApplyMsg
	votedFor    int

	electionTimer *time.Timer

	log []LogEntry

	// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	commitIndex int
	// index of highest log entry applied to state machine
	lastApplied int
	// zero-based
	// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	nextIndex []int
	// zero-based
	// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
// var term int
// var isleader bool
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		fmt.Printf("Decode from storage error.\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// had more recent info since it communicates the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	defer rf.persist()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		if logging {
			fmt.Printf("%d makes %d to follower on term %d\n", args.CandidateId, rf.me, args.Term)
		}
	}
	reply.Term = args.Term
	logUpToDate := false
	lastLogTerm := 0
	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rf.log)) {
		logUpToDate = true
	}
	if logUpToDate && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		if logging {
			fmt.Printf("%d votes for %d on term %d\n", rf.me, args.CandidateId, args.Term)
		}
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		// reset timer only when granting vote to someone.
		rf.resetElectionTimer()
	} else {
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < reply.Term {
		defer rf.persist()
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = Follower
	}
	return ok
}

type AppendEntriesArgs struct {
	// leader's term
	Term     int
	LeaderId int
	// index of log entry immediately preceding new ones
	PrevLogIndex int
	// term of prevLogIndex entry
	PrevLogTerm int
	// log entries to store (empty for heartbeat; may send more than one for efficiency)
	Entries []LogEntry
	// leader’s commitIndex
	LeaderCommit int
}

type AppendEntriesReply struct {
	// leader's term
	Term    int
	Success bool
	// term of follower which conflicts with the leader at PrevLogIndex.
	ConflictTerm int
	// first index of the conflict term.
	ConflictIndex int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if logging {
		fmt.Printf("%d receives heartbeat from %d on term %d, selfterm %d\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictTerm = -1
		reply.ConflictIndex = -1
		if logging {
			fmt.Printf("%d stale\n", args.LeaderId)
		}
		return
	}
	rf.resetElectionTimer()
	defer rf.persist()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if logging {
			fmt.Printf("%d forces %d to become follower, oterm: %d, selfterm: %d with role %d\n", args.LeaderId, rf.me, args.Term, rf.currentTerm, rf.role)
		}
		rf.role = Follower
	}

	reply.Term = rf.currentTerm
	// conflicts on PrevLogIndex with PrevLogTerm
	if args.PrevLogIndex >= 0 && (len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
		if args.PrevLogIndex >= len(rf.log) {
			reply.ConflictIndex = len(rf.log)
			reply.ConflictTerm = -1
		} else {
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			reply.ConflictIndex = args.PrevLogIndex
			for index := 0; index < reply.ConflictIndex; index++ {
				if rf.log[index].Term == reply.ConflictTerm {
					reply.ConflictIndex = index
					break
				}
			}
		}
		rf.log = rf.log[0:int(math.Min(float64(len(rf.log)), float64(args.PrevLogIndex)))]
		return
	}
	lastIndex := args.PrevLogIndex + len(args.Entries)
	if lastIndex < len(rf.log) {
		for i := 0; i < len(args.Entries); i++ {
			rf.log[args.PrevLogIndex+i+1] = args.Entries[i]
		}
	} else {
		rf.log = append(rf.log[0:args.PrevLogIndex+1], args.Entries...)
	}
	reply.Success = true
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(lastIndex)))
	}
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i + 1,
		}
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) sendAppendEntries(server int) {
	for {
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		lastIndex := len(rf.log) - 1
		prevIndex := rf.nextIndex[server] - 1
		prevTerm := -1
		if prevIndex >= 0 {
			prevTerm = rf.log[prevIndex].Term
		}
		args := AppendEntriesArgs{
			LeaderId:     rf.me,
			Term:         rf.currentTerm,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      rf.log[rf.nextIndex[server] : lastIndex+1],
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()
		reply := AppendEntriesReply{}
		if logging {
			fmt.Printf("%d sending AE to %d on term %d and index %d\n", rf.me, server, rf.currentTerm, rf.nextIndex[server])
		}
		ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
		if !ok {
			if logging {
				fmt.Printf("%d sending, %d has no response.\n", rf.me, server)
			}
			return
		}
		if reply.Success {
			// update indice if not heartbeat.
			if len(args.Entries) > 0 {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.matchIndex[server] = lastIndex
				rf.nextIndex[server] = lastIndex + 1
				for toCommit := lastIndex; toCommit > rf.commitIndex && rf.log[toCommit].Term == rf.currentTerm; toCommit-- {
					count := 0
					for index := range rf.peers {
						if rf.matchIndex[index] == toCommit {
							count++
						}
					}
					if count > len(rf.peers)/2 {
						rf.commitIndex = toCommit
						for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
							rf.applyCh <- ApplyMsg{
								CommandValid: true,
								Command:      rf.log[i].Command,
								CommandIndex: i + 1,
							}
						}
						rf.lastApplied = rf.commitIndex
						return
					}
				}
			}
			return
		}
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			defer rf.mu.Unlock()
			if logging {
				fmt.Printf("give up leader self: %d, selfterm: %d, objid:%d, objTerm:%d\n", rf.me, rf.currentTerm, server, reply.Term)
			}
			rf.role = Follower
			rf.currentTerm = reply.Term
			rf.persist()
			return
		}

		if reply.ConflictTerm == -1 {
			// don't update nextIndex if not caused by conflicting term, since that may result in race condition while re-elected.
			// this could happen when the leader already stepped down, and updated the current term,
			// but received from other peers for the requests with the old term.
			if reply.ConflictIndex != -1 {
				rf.nextIndex[server] = reply.ConflictIndex
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()
			return
		}
		// There are log conflicts, and nextIndex needs to be adjusted.
		rf.nextIndex[server] = reply.ConflictIndex
		for index := reply.ConflictIndex - 1; index >= 0; index-- {
			if rf.log[index].Term == reply.ConflictTerm {
				rf.nextIndex[server] = index + 1
				break
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartBeat() {
	for {
		rf.mu.Lock()
		isLeader := rf.role == Leader
		rf.mu.Unlock()
		if isLeader {
			for index := range rf.peers {
				if index != rf.me {
					// calls peers in parallel, and resign from the leader if self term is smaller.
					go func(server int) {
						rf.sendAppendEntries(server)
					}(index)
				}
			}
			time.Sleep(110 * time.Millisecond)
		} else {
			return
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return -1, -1, false
	}
	defer rf.persist()
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	rf.matchIndex[rf.me] = len(rf.log) - 1

	return len(rf.log), rf.currentTerm, true
}

// Kill: the tester doesn't halt goroutines created by Raft after each test,
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
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(200 * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.resetElectionTimer()

	if rf.role != Leader {
		// no heartbeat nor vote granted to others.
		defer rf.persist()
		rf.votedFor = rf.me
		rf.role = Candidate
		rf.currentTerm++
		rf.mu.Unlock()
		count := 0
		finished := 0
		if logging {
			fmt.Printf("%d starts a new election on term %d due to timeout with role %d.\n", rf.me, rf.currentTerm, rf.role)
		}

		var mu2 sync.Mutex
		cond := sync.NewCond(&mu2)

		lastLogTerm := 0
		if len(rf.log) > 0 {
			lastLogTerm = rf.log[len(rf.log)-1].Term
		}
		for index := range rf.peers {
			if index != rf.me {
				go func(i int) {
					args := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.log),
						LastLogTerm:  lastLogTerm,
					}
					reply := RequestVoteReply{}
					vote := rf.sendRequestVote(i, &args, &reply)
					mu2.Lock()
					defer mu2.Unlock()
					if vote && reply.VoteGranted {
						count++
					}
					finished++
					cond.Broadcast()
				}(index)
			}
		}
		mu2.Lock()
		for count < len(rf.peers)/2 && finished < len(rf.peers)-1 {
			cond.Wait()
		}
		rf.mu.Lock()
		if count >= len(rf.peers)/2 {
			rf.role = Leader
			for index := range rf.peers {
				rf.nextIndex[index] = len(rf.log)
				rf.matchIndex[index] = -1
			}
			if logging {
				fmt.Printf("%d becomes leader.\n", rf.me)
			}
			go rf.heartBeat()
		}
		mu2.Unlock()
	}
	rf.mu.Unlock()
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Reset(time.Duration(rand.Int31n(100)+300) * time.Millisecond)
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
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.role = Follower
	rf.votedFor = -1
	rf.electionTimer = time.NewTimer(time.Duration(rand.Int31n(50)) * time.Millisecond)

	go func() {
		for {
			<-rf.electionTimer.C
			rf.startElection()
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// go rf.ticker()

	return rf
}
