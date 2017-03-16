package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 2

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
    OpName string
    Key string
    NewValue string
    OldValue string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
    store map[string]string
    wait bool
    respCh chan raft.ApplyMsg
}


// the leader may be stale, don't consider it now
func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
    DPrintf("kv %v receive Get %v\n", kv.me, args)
    kv.mu.Lock()
    defer kv.mu.Unlock()
    cmd := Op{ OpName: "Get", Key: args.Key }
    index, _, ok := kv.rf.Start(cmd)
    if ok == false {
        reply.WrongLeader = true
        reply.Err = ErrWrongLeader
        return
    }
    DPrintf("kv %v receive Get %v wait for message\n", kv.me, args)
    kv.wait = true
    msg := <-kv.respCh
    DPrintf("kv %v receive Get %v obtain %v\n", kv.me, args, msg)
    _, ok2 := kv.rf.GetState()
    reply.WrongLeader = !ok2
    if msg.Index != index {
        reply.Err = ErrIndexNotMatch 
    } else if msg.Command != cmd {
        reply.Err = ErrCommandNotMatch
    } else {
        reply.Value = kv.store[args.Key]
        reply.Err = OK
    }
    DPrintf("kv %v receive Get %v reply %v\n", kv.me, args, reply)
    kv.wait = false
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
    kv.mu.Lock()
    defer kv.mu.Unlock()
    oldValue := kv.store[args.Key]
    // dummy, still has error
    cmd := Op{ OpName: args.Op, Key: args.Key, NewValue: args.Value,
        OldValue: oldValue }
    index, _, ok := kv.rf.Start(cmd)
    DPrintf("kv %v putappend %v index %v %v\n", kv.me, cmd, index, ok)
    if ok == false {
        reply.WrongLeader = true
        reply.Err = ErrWrongLeader
        return
    }
    DPrintf("kv %v putappend %v wait message\n", kv.me, cmd)
    kv.wait = true
    msg := <-kv.respCh
    DPrintf("kv %v PutAppend %v got %v\n", kv.me, cmd, msg)
    _, ok2 := kv.rf.GetState()
    reply.WrongLeader = !ok2
    if msg.Index != index {
        reply.Err = ErrIndexNotMatch
    } else if msg.Command != cmd {
        reply.Err = ErrCommandNotMatch
    } else {
        reply.Err = OK
    }
    DPrintf("kv %v putappend %v index %v : %v %v err %v\n", kv.me, cmd,
        index, msg.Index, msg.Command, reply.Err)
    kv.wait = false
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
    kv.store = make(map[string]string)
    kv.wait = false
    kv.respCh = make(chan raft.ApplyMsg)

    go func() {
        for {
            msg := <- kv.applyCh
            DPrintf("kv %v apply msg %v\n", kv.me, msg)
            op := msg.Command.(Op)
            switch op.OpName {
            case "Get":
            case "Put":
                kv.store[op.Key] = op.NewValue
            case "Append":
                concated := kv.store[op.Key] + op.NewValue
                kv.store[op.Key] = concated
            }
            DPrintf("before kv %v wait is %v\n", kv.me, kv.wait)
            if kv.wait {
                kv.respCh <- msg
            }
            DPrintf("after kv %v wait is %v\n", kv.me, kv.wait)
        }
    }()

	return kv
}
