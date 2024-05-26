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

	"log"
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
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
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

	role        Role
	currentTerm int
	votedFor    int //-1 if none
	//log         []LogEntry
	lastLogIndex int
	lastLogTerm  int

	commitIndex int //index of the highest log entry known to be committed
	lastApplied int //index of the highest log entry applied to state machine

	//for leader
	nextIndex  []int //index of the next log entry to send to each server, initialized to leader last log index + 1
	matchIndex []int //index of the highest log entry known to be replicated on server, initialized to 0

	//time when last heard from leader
	lastHeardFromLeader time.Time
	leaderId            int

	//votes recieved when server is candidate
	votesReceived map[int]bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

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
	// Your code here (3C).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Index        int //index of the candidate server
	CurrentTerm  int //term of the candidate server
	LastLogIndex int //index of the last log entry of the candidate server
	LastLogTerm  int //term of the last log entry of the candidate server
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Index       int  //index of the follower server
	VoteGranted bool //true if vote granted
	Term        int  //term of the follower server
}

type AppendEntriesArgs struct {
	Term         int        //leader's term
	LeaderId     int        //leaderId so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	LeaderCommit int        //leader's commitIndex
	Entries      []LogEntry //log entries to store (empty for heartbeat)
}

type AppendEntriesReply struct {
	Term    int  //currentTerm for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Println("Server ", rf.me, " received vote request from ", args.Index, " with term ", args.CurrentTerm)

	// Reply false if term < currentTerm
	if args.CurrentTerm < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

		log.Println("Server ", rf.me, " voted false to ", args.Index, " with term ", args.CurrentTerm)

		return
	}

	if args.CurrentTerm == rf.currentTerm {
		if rf.votedFor != -1 && rf.votedFor != args.Index {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm

			log.Println("Server ", rf.me, " voted false to ", args.Index, " with term ", args.CurrentTerm)

			return
		} else {
			rf.votedFor = args.Index
			reply.VoteGranted = true
			reply.Term = rf.currentTerm

			log.Println("Server ", rf.me, " voted true to ", args.Index, " with term ", args.CurrentTerm)

			return
		}
	} else if args.CurrentTerm < rf.currentTerm {
		//reply false if term < currentTerm
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

		log.Println("Server ", rf.me, " voted false to ", args.Index, " with term ", args.CurrentTerm)

		return
	} else {
		//convert to follower
		rf.Init(Follower, args.CurrentTerm, args.Index)
		log.Println("Server ", rf.me, " converted to follower from ", args.Index, " with term ", args.CurrentTerm)

		//vote if candidate's log is atleast as up-to-date as receiver's log
		if args.LastLogTerm > rf.lastLogTerm || (args.LastLogTerm == rf.lastLogTerm && args.LastLogIndex >= rf.lastLogIndex) {
			rf.votedFor = args.Index
			reply.VoteGranted = true
			reply.Term = rf.currentTerm

			log.Println("Server ", rf.me, " voted true to ", args.Index, " with term ", args.CurrentTerm)

			return
		} else {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm

			log.Println("Server ", rf.me, " voted false to ", args.Index, " with term ", args.CurrentTerm)
			return
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Println("Server ", rf.me, " received append entries from ", args.LeaderId, " with term ", args.Term)

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm

		log.Println("Server ", rf.me, " replied false to ", args.LeaderId, " with term ", args.Term)

		return
	}

	rf.lastHeardFromLeader = time.Now()

	//convert to follower if term > currentTerm
	if args.Term > rf.currentTerm {

		rf.Init(Follower, args.Term, args.LeaderId)
		log.Println("Server ", rf.me, " converted to follower from ", args.LeaderId, " with term ", args.Term)

		//TOOD: handle log entries
		return
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

func (rf *Raft) appendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

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

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		rf.TickerHandler()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 100 + (rand.Int63() % 50)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) TickerHandler() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	randomtime := rand.Intn(1500) + 1500

	if rf.role == Leader {
		//send heartbeat to all servers
		log.Println("Server ", rf.me, " sending heartbeat to all servers")
		args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.lastLogIndex, PrevLogTerm: rf.lastLogTerm, LeaderCommit: rf.commitIndex, Entries: []LogEntry{}}
		rf.SendAppendEntriesToAll(args)
	} else if time.Since(rf.lastHeardFromLeader) > time.Duration(randomtime)*time.Millisecond {
		rf.currentTerm++                       //increment currentTerm
		rf.Init(Candidate, rf.currentTerm, -1) //convert to candidate

		log.Println("Server ", rf.me, " starting election with term ", rf.currentTerm)

		args := RequestVoteArgs{Index: rf.me, CurrentTerm: rf.currentTerm, LastLogIndex: rf.lastLogIndex, LastLogTerm: rf.lastLogTerm}

		rf.SendRequestVoteToAll(args) //send request vote to all servers
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

	// Your initialization code here (3A, 3B, 3C).
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastLogIndex = 0
	rf.lastLogTerm = 0
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.votesReceived = make(map[int]bool)
	rf.lastHeardFromLeader = time.Now()
	rf.mu = sync.Mutex{}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	//log.SetOutput(ioutil.Discard)

	return rf
}

func (rf *Raft) SendRequestVoteToAll(args RequestVoteArgs) {

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				reply := RequestVoteReply{}

				log.Println("Server ", rf.me, " sending vote request to ", server, " with term ", rf.currentTerm)

				if rf.role != Candidate {
					log.Println("Server ", rf.me, " stopped sending vote request to ", server, " with term ", rf.currentTerm)
					return
				}

				rf.sendRequestVote(server, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.Init(Follower, reply.Term, -1) //convert to follower
					log.Println("Server ", rf.me, " converted to follower from ", server, " with term ", reply.Term)
				} else if reply.VoteGranted {

					if rf.role != Candidate {
						log.Println("Server ", rf.me, " is not a candidate anymore ", server, " with term ", rf.currentTerm)
						return
					}

					rf.votesReceived[server] = true

					log.Println("Server ", rf.me, " received vote from ", server, " with term ", rf.currentTerm, " and votesReceived ", len(rf.votesReceived))

					if len(rf.votesReceived) > len(rf.peers)/2 {
						rf.Init(Leader, rf.currentTerm, rf.me) //convert to leader
						args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.lastLogIndex, PrevLogTerm: rf.lastLogTerm, LeaderCommit: rf.commitIndex, Entries: []LogEntry{}}
						rf.SendAppendEntriesToAll(args) //send heartbeat to all servers
						log.Println("Server ", rf.me, " converted to leader with term ", rf.currentTerm)
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) Init(role Role, term int, leaderId int) {
	rf.role = role
	rf.votedFor = -1
	rf.currentTerm = term
	rf.leaderId = leaderId
	rf.votesReceived = make(map[int]bool)
	rf.lastHeardFromLeader = time.Now()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	log.Println("Server ", rf.me, " initialized with role ", role, " and term ", term, " and leaderId ", leaderId)

	if role == Leader {
		for i := range rf.nextIndex {
			rf.nextIndex[i] = rf.lastLogIndex + 1
		}
		for i := range rf.matchIndex {
			rf.matchIndex[i] = 0
		}
	} else if role == Candidate {
		rf.votedFor = rf.me
		rf.votesReceived[rf.me] = true
	}
}

func (rf *Raft) SendAppendEntriesToAll(args AppendEntriesArgs) {

	log.Println("Server ", rf.me, " starting sending append entries to all servers")

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				reply := AppendEntriesReply{}

				if rf.role != Leader {
					log.Println("Server ", rf.me, " sstopped ending append entries to all servers")
					return
				}

				log.Println("Server ", rf.me, " sending append entries to ", server, " with term ", rf.currentTerm)
				rf.appendEntries(server, &args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm { //TODO: check if this is correct
					rf.Init(Follower, reply.Term, -1) //convert to follower
					log.Println("Server ", rf.me, " converted to follower from ", server, " with term ", reply.Term)
				}
			}(i)
		}
	}
}
