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

import "sync"
import "labrpc"
import "time"
import "math/rand"
// import "fmt"
import "log"

// import "bytes"
// import "encoding/gob"

// timeout related constants
const (
	TIMEOUT_HB = 200 * time.Millisecond
	TIMEOUT_LOW = 3 * TIMEOUT_HB
	TIMEOUT_HIGH = TIMEOUT_LOW + TIMEOUT_HB
)

// raft's state constants
const (
	FOLLOWER int = iota
	CANDIDATE
	LEADER
)

func getRandTimeout() time.Duration {
	return time.Duration(rand.Int63n(TIMEOUT_HIGH.Nanoseconds() -
        TIMEOUT_LOW.Nanoseconds()) + TIMEOUT_LOW.Nanoseconds())
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int // latest term server has sendRequestVote
	votedFor int // candidateId that received vote int current term
	log []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine
	nextIndex []int // for each server, idnex of the next log entry to send to that server
	matchIndex []int // for each server, idnex of highest log entry known to be replicated on server
    tc string // just for debug

    state int
    voteCount int

    // some channels used to communicate information
    heartBeatCh chan bool // include hb and append entries
    voteGrantCh chan bool // receive requestvote
    inauguralCh chan bool // got votes from a majority and becomes the new leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

type LogEntry struct {
	Term int
	Cmd interface{}
}

type AppendEntriesArgs struct {
	Term int // leader's term
	LeaderId int // so followers can redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm int // term of prevLogIndex entry
	Entries []LogEntry // log entries to store
	LeaderCommit int // leader's commitIndex
    Tc string
}

type AppendEntriesReply struct {
	Term int // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// NOTE: if you use AppendEntries(*AppendEntriesArgs, *AppendEntriesReply),
// then the receiver AppendEntries will receive args *AppendEntriesArgs as nil
// but I don't know why still
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    // - delayed packet from last leader
    // - stale leader 
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	reply.Term = args.Term
	reply.Success = true
	// for _, entry := range args.Entries {
	// 	// replicate logs
	// }
	rf.currentTerm = args.Term
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.heartBeatCh <- true
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs,
        reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int // candidate's term
	CandidateId int // candidate id
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm int // term of candidate's last log entry
    Tc string
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int // currentTerm, for candidate to update itself
	VoteGranted bool // accept a candidate as leader
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    rf.mu.Lock()  
  	defer rf.mu.Unlock()

    if args.Term < rf.currentTerm {
    	reply.Term = rf.currentTerm
    	reply.VoteGranted = false
    	return
    }
    if args.Term > rf.currentTerm {
    	rf.currentTerm = args.Term
    	rf.votedFor = -1
    	rf.state = FOLLOWER
    }
    lastLogIndex := len(rf.log) - 1
    lastLogTerm := rf.log[lastLogIndex].Term
    if rf.votedFor == -1 && (args.LastLogTerm > lastLogTerm ||
            (args.LastLogTerm == lastLogTerm &&
            args.LastLogIndex >= lastLogIndex)) {
    	rf.votedFor = args.CandidateId
    	reply.VoteGranted = true
    	rf.voteGrantCh <- true
    } else {
    	reply.VoteGranted = false
    }
}

//
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
// the struct itself.T
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

var counter int = 0

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
    rf.currentTerm = 0
    rf.tc = string('A' + counter)
    if me + 1 == len(peers) {
        counter++
    }
    // dummy
    rf.log = append(rf.log, LogEntry{})
    // if you don't make channel manually, then it will stuck
    // is it necessary to use buffered channel?
    rf.heartBeatCh = make(chan bool)
    rf.voteGrantCh = make(chan bool)
    rf.inauguralCh = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

    log.SetFlags(log.Ltime | log.Lmicroseconds)

    go func() {
    	for {
    		switch rf.state {
    		case FOLLOWER:
    			select {
    			case <-rf.heartBeatCh:
    			case <-rf.voteGrantCh:
    			case <-time.After(getRandTimeout()):
    				rf.state = CANDIDATE // need protection?
    			}
    		case CANDIDATE:
    			rf.mu.Lock()
    			rf.currentTerm++
    			rf.votedFor = rf.me
    			rf.voteCount = 1
    			rf.mu.Unlock()
    			go rf.startElection()
    			select {
    			case <-rf.heartBeatCh:
    			case <-rf.voteGrantCh:
    			case <-rf.inauguralCh:
    			case <-time.After(getRandTimeout()):
    			}
    		case LEADER:
    			go rf.startHeartBeat()
    			select {
    			case <-rf.heartBeatCh:
    			case <-rf.voteGrantCh:
    			case <-time.After(TIMEOUT_HB):
    			}
    		}
    	}
    }()
	
	return rf
}

// startElection and startHeartBeat are mutual exclusive
func (rf *Raft) startElection() {
	args:= RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
    args.Tc = rf.tc
	for i := 0; i < len(rf.peers) && rf.state == CANDIDATE; i++ {
        if i == rf.me { continue }
		go func(server int) {
            reply := RequestVoteReply{}
            ok := rf.sendRequestVote(server, &args, &reply)
			if ok && reply.VoteGranted {
				rf.mu.Lock()
				rf.voteCount++
                // use ==, otherwise this may trigger multiple times
				if rf.voteCount == len(rf.peers) / 2 + 1{
                    rf.state = LEADER
                    rf.votedFor = -1
					rf.inauguralCh <- true
				}
				rf.mu.Unlock()
			}
			if reply.Term > rf.currentTerm {
				// trun to follower state
				rf.mu.Lock()
				rf.currentTerm = reply.Term
                rf.votedFor = -1
				rf.state = FOLLOWER
				rf.mu.Unlock()
			}
		}(i)
	}
}

// heart beat message with log entries carried.
func (rf *Raft) startHeartBeat() {
	hb := AppendEntriesArgs{}
	hb.Term = rf.currentTerm
	hb.LeaderId = rf.me
	hb.Entries = nil
    hb.Tc = rf.tc
    for i := range rf.peers {
        if i == rf.me { continue }
        // if don't invoke this function with the heartbeat as argument,
        // as go func(int), then you may get unexpected value when reference
        // hb in the new thread after this thread exit.
		go func(server int, args2 AppendEntriesArgs) {
            var args AppendEntriesArgs
            args.Term = rf.currentTerm
            args.LeaderId = rf.me
            args.PrevLogIndex = 0
            args.PrevLogTerm = 0
            // args.Entries = make([]LogEntry, 0)
            // args.Entries = nil
			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(server, args, &reply) &&
                    reply.Term > rf.currentTerm {
				rf.mu.Lock()
				rf.currentTerm = reply.Term
                rf.votedFor = -1
				rf.mu.Unlock()
			}
		}(i, hb)
	}
    time.Sleep(1 * time.Second)

}

