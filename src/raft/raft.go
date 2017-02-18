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
import "fmt"

// import "bytes"
// import "encoding/gob"

const (
	HEARTBEAT = 200
	TIMEOUT_L = 3 * HEARTBEAT
	TIMEOUT_H = TIMEOUT_L + HEARTBEAT
)

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
	leaderId int // leader id
	currentTerm int // latest term server has sendRequestVote
	votedFor int // candidateId that received vote int current term
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine
	nextIndex []int // for each server, idnex of the next log entry to send to that server
	matchIndex []int // for each server, idnex of highest log entry known to be replicated on server
	timer *time.Timer // timer used to do timeout
	r *rand.Rand // rand
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.me == rf.leaderId
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

}

type AppendEntriesArgs struct {
	Term int // leader's term
	LeaderId int // so followers can redirect clients
	Prevlogindex int // index of log entry immediately preceding new ones
	Prevlogterm int // term of prevLogIndex entry
	Entries []LogEntry // log entries to store
	Leadercommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term int // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
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
	rf.mu.Lock()
    rf.leaderId = args.LeaderId
	rf.timer.Stop()
	rf.timer = time.AfterFunc(time.Millisecond * time.Duration(rf.r.Intn(TIMEOUT_H - TIMEOUT_L) + TIMEOUT_L), rf.startElection)
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs,
        reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int // currentTerm, for candidate to update itself
	Accept bool // accept a candidate as leader
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    rf.mu.Lock()
    if args.Term > rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor == -1)  {
        rf.currentTerm = args.Term
        rf.votedFor = args.CandidateId
        rf.timer.Stop()
        rf.timer = time.AfterFunc(time.Millisecond * time.Duration(rf.r.Intn(TIMEOUT_H - TIMEOUT_L) + TIMEOUT_L), rf.startElection)
		reply.Accept = true
		reply.Term = args.Term
        fmt.Printf("peer %v(%v) vote candidate %v(%v)\n", rf.me,
            rf.currentTerm, args.CandidateId, args.Term)
    } else {
		reply.Accept = false
		reply.Term = rf.currentTerm
        fmt.Printf("peer %v(%v) vote candidate %v(%v) voted for %v\n", rf.me,
            rf.currentTerm, args.CandidateId, args.Term, rf.votedFor)
	}
    rf.mu.Unlock()
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
	rf.leaderId = -1
	rf.r = rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.currentTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.mu.Lock()
	rf.timer = time.AfterFunc(time.Millisecond * time.Duration(rf.r.Intn(TIMEOUT_H - TIMEOUT_L) + TIMEOUT_L), rf.startElection)
	rf.mu.Unlock()
	return rf
}

// startElection and startHeartBeat are mutual exclusive
func (rf *Raft) startElection() {
	rf.mu.Lock()
    rf.currentTerm++
    rf.votedFor = rf.me
	rf.timer.Stop()
	rf.timer = time.AfterFunc(time.Millisecond * time.Duration(rf.r.Intn(TIMEOUT_H - TIMEOUT_L) + TIMEOUT_L), rf.startElection)
	rf.mu.Unlock()
	args:= RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	votes := 1
	wg := sync.WaitGroup{}
	wg.Add(len(rf.peers))
    // fmt.Printf("in term %v before raft %v requestvote\n", rf.currentTerm, rf.me)
	for i := range rf.peers {
		reply := RequestVoteReply{}
		go func(server int) {
			if rf.sendRequestVote(server, &args, &reply) && reply.Accept {
				rf.mu.Lock()
				votes++
				rf.mu.Unlock()
			}
			wg.Done()
			if reply.Term > rf.currentTerm {
				// trun to follower state
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.timer.Stop()
				rf.timer = time.AfterFunc(time.Millisecond * time.Duration(rf.r.Intn(TIMEOUT_H - TIMEOUT_L) + TIMEOUT_L), rf.startElection)
				rf.mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
    fmt.Printf("* in term %v raft %v got %v votes\n", rf.currentTerm, rf.me, votes)
	if votes > len(rf.peers) / 2 {
		rf.mu.Lock()
		rf.timer.Stop()
        rf.leaderId = rf.me
		rf.mu.Unlock()
		go rf.startHeartBeat()
	}
}

func (rf *Raft) startHeartBeat() {
    fmt.Printf("in term %v raft %v start to send heartbeat\n", rf.currentTerm,
        rf.me)
	rf.mu.Lock()
	rf.timer.Stop()
	rf.timer = time.AfterFunc(time.Millisecond * time.Duration(HEARTBEAT), rf.startElection)
	rf.mu.Unlock()
	hb := AppendEntriesArgs{}
	hb.Term = rf.currentTerm
	hb.LeaderId = rf.me
	hb.Entries = nil
    for i := range rf.peers {
		go func(server int) {
			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(server, &hb, &reply) &&
                    reply.Term > rf.currentTerm {
				rf.mu.Lock()
				rf.currentTerm = reply.Term
                rf.votedFor = -1
				rf.timer.Stop()
				rf.timer = time.AfterFunc(time.Millisecond * time.Duration(rf.r.Intn(TIMEOUT_H - TIMEOUT_L) + TIMEOUT_L), rf.startElection)
				rf.mu.Unlock()
			}
		}(i)
	}
    fmt.Printf("in term %v raft %v finished sending heartbeat\n", rf.currentTerm,
        rf.me)
}

