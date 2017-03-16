package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
    leaderId int
    id int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

var count int = 0

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
    ck.leaderId = 0
    ck.id = count
    count++
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
    DPrintf("ck %v receive get %v\n", ck.id, key)
    succ := false
    value := ""
    for i := ck.leaderId; succ == false; i++ {
        srv := i % len(ck.servers)
        args := GetArgs{ key }
        reply := GetReply{}
        ok := ck.servers[srv].Call("RaftKV.Get", &args, &reply)
        DPrintf("ck %v to kv %v Get %v RPC %v reply %v\n", ck.id, srv, key,
            ok, reply)
        if ok == false {
            DPrintf("ck %v to kv %v Get %v RPC error\n", ck.id, srv, key)

        } else if reply.Err == OK {
            DPrintf("ck %v to kv %v Get %v succ\n", ck.id, srv, key)
            succ = true
            value = reply.Value
            if reply.WrongLeader == false {
                ck.leaderId = srv
            }
        } else {
            DPrintf("ck %v to kv %v Get %v error %v\n", ck.id, srv, key, reply.Err)
        }
    }
    DPrintf("ck %v exit get %v:%v\n", ck.id, key, value)

	return value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
    succ := false
    for i :=  ck.leaderId; succ == false; i++ {
        srv := i % len(ck.servers)
        args := PutAppendArgs{ key, value, op }
        reply := PutAppendReply{}
        DPrintf("now try server %v\n", srv)
        ok := ck.servers[srv].Call("RaftKV.PutAppend", &args, &reply)
        if ok == false {
            DPrintf("ck %v to kv %v %v %v RPC error\n", ck.id, srv, op, key)
        } else if reply.Err == OK {
            DPrintf("ck %v to kv %v %v %v succ\n", ck.id, srv, op, key)
            succ = true
            if reply.WrongLeader == false {
                ck.leaderId = srv
            }
        } else {
            DPrintf("ck %v to kv %v %v %v error %v\n", ck.id, srv, op, key,
                reply.Err)
        }
    }
}

func (ck *Clerk) Put(key string, value string) {
    DPrintf("ck %v receive put %v:%v\n", ck.id, key, value)
	ck.PutAppend(key, value, "Put")
    DPrintf("ck %v exit put %v:%v\n", ck.id, key, value)
}
func (ck *Clerk) Append(key string, value string) {
    DPrintf("ck %v receive append %v:%v\n", ck.id, key, value)
	ck.PutAppend(key, value, "Append")
    DPrintf("ck %v exit append %v:%v\n", ck.id, key, value)
}
