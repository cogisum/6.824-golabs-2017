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

import "bytes"
import "encoding/gob"

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

const (
    STEPBACK = 30
)

func getRandTimeout() time.Duration {
	return time.Duration(rand.Int63n(TIMEOUT_HIGH.Nanoseconds() -
        TIMEOUT_LOW.Nanoseconds()) + TIMEOUT_LOW.Nanoseconds())
}

func min(a int, b int) int {
    if a < b {
        return a
    } else {
        return b
    }
}

func max(a int, b int) int {
    if a < b {
        return b
    } else {
        return a
    }
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

    state int
    voteCount int

    // some channels used to communicate information
    heartBeatCh chan bool // include hb and append entries
    voteGrantCh chan bool // receive requestvote
    inauguralCh chan bool // got votes from a majority and becomes the new leader
    applyCh chan ApplyMsg
    commitCh chan bool
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

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
    e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
    d.Decode(&rf.log)
}

func (rf *Raft) getLastLogInfo() (int, int) {
    lastLogIndex := len(rf.log) - 1
    lastLogTerm := rf.log[lastLogIndex].Term
    return lastLogIndex, lastLogTerm
}

type LogEntry struct {
	Term int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term int // leader's term
	LeaderId int // so followers can redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm int // term of prevLogIndex entry
	Entries []LogEntry // log entries to store
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term int // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
    ConflictTerm int
    FirstIndex  int
}

func (rf *Raft) getFirstIndexOfTerm(term int) int {
    index := len(rf.log) - 1;
    for index >= 0 && rf.log[index].Term >= term {
        index--
    }
    return index + 1
}

// NOTE: if you use AppendEntries(*AppendEntriesArgs, *AppendEntriesReply),
// then the receiver AppendEntries will receive args *AppendEntriesArgs as nil
// but I still don't know why
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    DPrintf("raft %v(%v) receive appendentries from %v\n", rf.me,
        rf.currentTerm, args.LeaderId)
    rf.mu.Lock()
    defer rf.mu.Unlock()
    defer DPrintf("raft %v(%v) receive appendentries from %v\n", rf.me,
        rf.currentTerm, args.LeaderId)

    lastLogIndex, lastLogTerm := rf.getLastLogInfo()


	reply.Term = args.Term

    // - delayed packet from last leader
    // - stale leader 
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.currentTerm = args.Term
    if args.PrevLogIndex > lastLogIndex {
        reply.Success = false
        reply.ConflictTerm = lastLogTerm
        reply.FirstIndex = rf.getFirstIndexOfTerm(lastLogTerm)
    } else if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
        reply.Success = false
        reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
        reply.FirstIndex = rf.getFirstIndexOfTerm(reply.ConflictTerm)
    } else {
        rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
        rf.persist()
        reply.Success = true
        if args.LeaderCommit > rf.commitIndex {
            if args.LeaderCommit < len(rf.log) {
                rf.commitIndex = args.LeaderCommit
            } else {
                rf.commitIndex = len(rf.log) - 1
            }
            DPrintf("raft %v(%v) receive appendentries from %v before" +
                " commit [%v, %v]\n", rf.me, rf.currentTerm, args.LeaderId,
                rf.lastApplied + 1, rf.commitIndex)
            rf.commitCh <- true
            DPrintf("raft %v(%v) receive appendentries from %v after" +
                " commit [%v, %v]\n", rf.me, rf.currentTerm, args.LeaderId,
                rf.lastApplied + 1, rf.commitIndex)
        }
    }
	rf.state = FOLLOWER
	// rf.votedFor = args.LeaderId
    if reply.Success {
        DPrintf("%v(%v) receive appendentries from %v and accept it: %v\n",
            rf.me, rf.currentTerm, args.LeaderId, args.Entries)

    } else {
        DPrintf("%v(%v) receive appendentries from %v and deny it: %v\n",
            rf.me, rf.currentTerm, args.LeaderId, args.Entries)

    }
    DPrintf("raft %v(%v) receive appendentries from %v before heartbeat\n", rf.me,
        rf.currentTerm, args.LeaderId)
	rf.heartBeatCh <- true
    DPrintf("raft %v(%v) receive appendentries from %v after heartbeat\n", rf.me,
        rf.currentTerm, args.LeaderId)
}

func (rf *Raft) getCommand() []interface{} {
    var commands []interface{}
    for _, entry := range rf.log {
        commands = append(commands, entry.Command)
    }
    return commands
}

func (rf *Raft) truncateLog(to int) {
    if len(rf.log) >= to {
        rf.log = rf.log[:to]
    }
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
    DPrintf("raft %v(%v) receive requestvote from %v\n", rf.me, rf.currentTerm,
        args.CandidateId)
    rf.mu.Lock()  
  	defer rf.mu.Unlock()
    defer DPrintf("raft %v(%v) exit requestvote from %v\n", rf.me,
        rf.currentTerm, args.CandidateId)

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
    lastLogIndex, lastLogTerm := rf.getLastLogInfo()
    if rf.votedFor == -1 && (args.LastLogTerm > lastLogTerm ||
            (args.LastLogTerm == lastLogTerm &&
            args.LastLogIndex >= lastLogIndex)) {
    	rf.votedFor = args.CandidateId
        rf.persist()
    	reply.VoteGranted = true
        DPrintf("raft %v(%v) receive requestvote from %v before vote\n",
            rf.me, rf.currentTerm, args.CandidateId)
    	rf.voteGrantCh <- true
        DPrintf("raft %v(%v) receive requestvote from %v after vote\n",
            rf.me, rf.currentTerm, args.CandidateId)
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
	// Your code here (2B).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    DPrintf("raft %v(%v) receive command %v\n", rf.me, rf.currentTerm, command)
    var index int
    term, isLeader := rf.GetState()
    if isLeader {
        // respond immediately without agreement from a majority?
        // what if this leader fail?
        index = len(rf.log)
        rf.log = append(rf.log, LogEntry{ term, command })
        rf.persist()
    }

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
    rf.applyCh = applyCh
    if me + 1 == len(peers) {
        counter++
    }
    rf.commitIndex = 0
    rf.lastApplied = 0
    rf.currentTerm = 0
	rf.votedFor = -1
    rf.log = []LogEntry{ LogEntry{ 0, "Genesis" } }
    // if you don't make channel manually, then it will stuck
    // is it necessary to use buffered channel?
    rf.heartBeatCh = make(chan bool, 10)
    rf.voteGrantCh = make(chan bool)
    rf.inauguralCh = make(chan bool)
    rf.commitCh = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

    log.SetFlags(log.Ltime | log.Lmicroseconds)

    // state transition
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
    			go rf.startElection()
    			select {
    			case <-rf.heartBeatCh:
    			case <-rf.voteGrantCh:
    			case <-rf.inauguralCh:
                    lastLogIndex, _ := rf.getLastLogInfo()
                    rf.nextIndex = make([]int, len(rf.peers))
                    rf.matchIndex = make([]int, len(rf.peers))
                    for i := 0; i < len(rf.peers); i++ {
                        rf.nextIndex[i] = lastLogIndex + 1
                        rf.matchIndex[i] = 0
                    }
                    rf.state = LEADER
                    DPrintf("raft %v(%v) became leader\n", rf.me, rf.currentTerm)
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

    // apply committed log
    go func() {
        for {
            select {
            case <-rf.commitCh:
                for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
                    applyMsg := ApplyMsg{}
                    applyMsg.Index = i
                    applyMsg.Command = rf.log[i].Command
                    applyCh <- applyMsg
                    DPrintf("%v(%v) apply %v\n", rf.me, rf.currentTerm, i)
                    rf.lastApplied++
                }
            }
        }
    }()
	
	return rf
}

// startElection and startHeartBeat are mutual exclusive
func (rf *Raft) startElection() {
    rf.mu.Lock()
    rf.currentTerm++
    rf.votedFor = rf.me
    rf.persist()
    rf.voteCount = 1
	args:= RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
    args.LastLogIndex, args.LastLogTerm = rf.getLastLogInfo()
    DPrintf("raft %v(%v) start election\n", rf.me, rf.currentTerm)
    rf.mu.Unlock()
	for i := 0; i < len(rf.peers) && rf.state == CANDIDATE; i++ {
        if i == rf.me { continue }
		go func(server int) {
            reply := RequestVoteReply{}
            ok := rf.sendRequestVote(server, &args, &reply)
            rf.mu.Lock()
            defer rf.mu.Unlock()
			if ok && reply.VoteGranted {
                // discard if stale
                if rf.currentTerm == args.Term {
                    rf.voteCount++
                    // use ==, otherwise this may trigger multiple times
                    if rf.voteCount == len(rf.peers) / 2 + 1 {
                        rf.inauguralCh <- true
                    }
                }
			}
			if reply.Term > rf.currentTerm {
				// trun to follower state
				rf.currentTerm = reply.Term
                rf.votedFor = -1
				rf.state = FOLLOWER
			}
		}(i)
	}
}

// heart beat message with log entries carried.
func (rf *Raft) startHeartBeat() {
    // trick, how to know a log entry committed? count how many servers the log
    // was replicated on
    // only log entries from the leader's current term are committed by
    // counting replicas
    rf.mu.Lock()
    defer rf.mu.Unlock()
    DPrintf("raft %v(%v) start heartbeat commit %v, len %v\n", rf.me,
        rf.currentTerm, rf.commitIndex, len(rf.log))

    for i := len(rf.log) - 1; i > rf.commitIndex &&
            rf.log[i].Term == rf.currentTerm; i-- {
        count := 0
        for _, replicated := range rf.matchIndex {
            if replicated >= i {
                count++
            }
        }
        if count >= len(rf.peers) / 2 {
            rf.commitIndex = i
            rf.commitCh <- true
            break
        }
    }

    term := rf.currentTerm

    for i := 0; i < len(rf.peers) && rf.state == LEADER; i++ {
        if i == rf.me { continue }
        // if don't invoke this function with the heartbeat as argument,
        // as go func(int), then you may get unexpected value when reference
        // hb in the new thread after this thread exit.
        var args AppendEntriesArgs
        args.Term = term
        args.LeaderId = rf.me
        args.LeaderCommit = rf.commitIndex
        args.PrevLogIndex = rf.nextIndex[i] - 1
        args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
        args.Entries = make([]LogEntry, len(rf.log) - rf.nextIndex[i])
        copy(args.Entries, rf.log[rf.nextIndex[i]:])
		go func(server int) {
			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(server, args, &reply) {
                rf.mu.Lock()
                defer rf.mu.Unlock()
                if reply.Term > rf.currentTerm {
                    rf.currentTerm = reply.Term
                    rf.votedFor = -1
                    rf.state = FOLLOWER
                } else if reply.Success {
                    rf.nextIndex[server] = max(rf.nextIndex[server],
                        args.PrevLogIndex + len(args.Entries) + 1)
                    rf.matchIndex[server] = rf.nextIndex[server] - 1
                } else {
                    firstIndex := rf.getFirstIndexOfTerm(reply.ConflictTerm)
                    if firstIndex == len(rf.log) ||
                            rf.log[firstIndex].Term == reply.ConflictTerm {
                        rf.nextIndex[server] = firstIndex
                    } else {
                        rf.nextIndex[server] = reply.FirstIndex
                    }
                    rf.nextIndex[server] = max(rf.matchIndex[server] + 1,
                        rf.nextIndex[server])
                }
			}
		}(i)
	}

}


/*
Make中state CANDIDATE时加锁/接收chan与requestvote和appendentries加锁/发送chan死锁示例：

case 1
以requestvote为例，（appendentries同）
1. 收到requestvote中term为+1，然后voteGrantCh<-
2. 收到requestvote中term为+2，然后voteGrantCh<-
...直到把voteGrantCh的buffer用完，（虽然在buffer很大时不太可能出现这种情况）
再次收到requestvote，然后timeout，切换到candidate状态，由于requestvote中申请的锁
还没有释放，所以candidate等待获取锁，接下来会再次收到requestvote或appendentries，
同样地会在申请锁时阻塞。

case 2
server in Make case state == CANDIDATE
spot 1. AppendEntries
收到appendentries，得到锁，heartBeatCh<-，由于对heartBeatCh的接收要在spot 2中更新完rf.currentTerm后才会执行，所以现在阻塞，并且不会返回给leader，所以leader会将nextIndex后退，虽然这个server已经成功收到并且可能已经apply了。同事在appendentries中，虽然已经将server的state改为follower，但是由于spot2一直阻塞，所以并没有机会重新进入for以应用follower状态。
spot 2. State Loop
申请锁以更新rf.currentTerm和rf.votedFor，阻塞

case 3
同样地inauguralCh与lock也能造成死锁。

solution：
1. case CANDIDATE后要加锁的部分在一个goroutine中进行，保证安全，然而线程的创建/回收开销较大（补充，将其放到startElection中）
2. 令voteGrantCh和heartBeatCh的缓冲区较大，比如100，这样虽然不能保证安全（如case 1缓冲区被消耗完），但是出现死锁的概率应该极小。
 */


