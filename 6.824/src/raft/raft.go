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
import "math/rand"
import "time"
import "fmt"

// import "bytes"
// import "encoding/gob"

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
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm     int
	votedFor        int
	state           int // 0 is follower | 1 is candidate | 2 is leader
	timeout         int
	currentLeader   int
	lastMessageTime int64
	message         chan bool
	electCh         chan bool
	heartbeat       chan bool
	heartbeatRe     chan bool
}

func (rf *Raft) becomeCandidate() {
	rf.state = 1
	rf.setTerm(rf.currentTerm + 1)
	rf.votedFor = rf.me
	rf.currentLeader = -1
}

func (rf *Raft) becomeLeader() {
	rf.state = 2
	rf.currentLeader = rf.me
}

func (rf *Raft) becomeFollower(term int, candidate int) {
	rf.setTerm(term)
	rf.votedFor = candidate
	rf.setMessageTime(milliseconds())
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here.
	term = rf.currentTerm
	isLeader = rf.currentLeader == rf.me
	return term, isLeader
}

func (rf *Raft) setTerm(term int) {
	printTime()
	fmt.Printf("server = %d update term = %d\n", rf.me, term)
	rf.currentTerm = term
}

func (rf *Raft) setMessageTime(time int64) {
	//printTime()
	//fmt.Printf("candidate=%d set last message time=%d\n", rf.me, time)
	rf.lastMessageTime = time
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
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
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func printTime() {
	t := time.Now()
	fmt.Printf("%s ", t.String())
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	currentTerm, _ := rf.GetState()
	if args.Term < currentTerm {
		reply.Term = currentTerm
		reply.VoteGranted = false
		printTime()
		fmt.Printf("candidate=%d term = %d smaller than server = %d, currentTerm = %d\n", args.CandidateId, args.Term, rf.me, rf.currentTerm)
		return
	}

	if rf.votedFor != -1 && args.Term <= rf.currentTerm {
		reply.VoteGranted = false
		rf.mu.Lock()
		rf.setTerm(max(args.Term, currentTerm))
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		printTime()
		fmt.Printf("rejected candidate=%d term = %d server = %d, currentTerm = %d, has_voted_for = %d\n", args.CandidateId, args.Term, rf.me, rf.currentTerm, rf.votedFor)
	} else {
		rf.mu.Lock()
		rf.becomeFollower(max(args.Term, currentTerm), args.CandidateId)
		rf.mu.Unlock()
		reply.VoteGranted = true
		fmt.Printf("accepted server = %d voted_for candidate = %d\n", rf.me, args.CandidateId)
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		reply.Success = true
		reply.Term = rf.currentTerm
		rf.mu.Lock()
		rf.currentLeader = args.LeaderId
		rf.votedFor = args.LeaderId
		rf.state = 0
		rf.setMessageTime(milliseconds())
		printTime()
		fmt.Printf("server = %d learned that leader = %d\n", rf.me, rf.currentLeader)
		rf.mu.Unlock()
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeat(server int, args AppendEntriesArgs, reply *AppendEntriesReply, timeout int) {
	c := make(chan bool, 1)
	go func() { c <- rf.peers[server].Call("Raft.AppendEntries", args, reply) }()
	select {
	case ok := <-c:
		if ok && reply.Success {
			printTime()
			fmt.Printf("candidate=%d receive heartbeat confirm from server=%d\n", rf.me, server)
			rf.heartbeatRe <- true
		} else {
			printTime()
			fmt.Printf("candidate=%d does not receive heartbeat confirm from server=%d\n", rf.me, server)
			rf.heartbeatRe <- false
		}
	case <-time.After(time.Duration(timeout) * time.Millisecond):
		printTime()
		fmt.Printf("candidate=%d timeout heartbeat confirm from server=%d\n", rf.me, server)
		rf.heartbeatRe <- false
		break
	}
}

func randomRange(min, max int64) int64 {
	return rand.Int63n(max-min) + min
}

func milliseconds() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func (rf *Raft) sendRequestVoteAndTrigger(server int, args RequestVoteArgs, reply *RequestVoteReply, timeout int) {
	c := make(chan bool, 1)
	go func() { c <- rf.sendRequestVote(server, args, reply) }()
	select {
	case ok := <-c:
		if ok && reply.VoteGranted {
			rf.electCh <- true
			printTime()
			fmt.Printf("candidate=%d receive vote from server=%d\n", rf.me, server)
		} else {
			rf.electCh <- false
			printTime()
			fmt.Printf("candidate=%d receive unvote from server=%d, ok = %d\n", rf.me, server, ok)
		}
	case <-time.After(time.Duration(timeout) * time.Millisecond):
		printTime()
		fmt.Printf("candidate=%d timeout vote confirm from server=%d\n", rf.me, server)
		rf.electCh <- false
		break
	}
}

func (rf *Raft) sendAppendEntriesImpl() {
	if rf.currentLeader == rf.me {
		var args AppendEntriesArgs
		var success_count int
		timeout := 20
		args.LeaderId = rf.me
		args.Term = rf.currentTerm
		printTime()
		fmt.Printf("broadcast heartbeat start\n")
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				var reply AppendEntriesReply
				printTime()
				fmt.Printf("Leader=%d send heartbeat to server=%d\n", rf.me, i)
				go rf.sendHeartBeat(i, args, &reply, timeout)
			}
		}
		for i := 0; i < len(rf.peers)-1; i++ {
			select {
			case ok := <-rf.heartbeatRe:
				if ok {
					success_count++
					if success_count >= len(rf.peers)/2 {
						rf.mu.Lock()
						rf.setMessageTime(milliseconds())
						rf.mu.Unlock()
					}
				}
			}
		}
		printTime()
		fmt.Printf("broadcast heartbeat end\n")
		if success_count < len(rf.peers)/2 {
			rf.mu.Lock()
			rf.currentLeader = -1
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendLeaderHeartBeat() {
	timeout := 20
	for {
		select {
		case <-rf.heartbeat:
			rf.sendAppendEntriesImpl()
		case <-time.After(time.Duration(timeout) * time.Millisecond):
			rf.sendAppendEntriesImpl()
		}
	}
}

func (rf *Raft) election_one_round() bool {
	// begin election
	var timeout int64
	var done int
	var triggerHeartbeat bool
	timeout = 100
	last := milliseconds()
	success := false
	rf.mu.Lock()
	rf.becomeCandidate()
	rf.mu.Unlock()
	printTime()
	rpcTimeout := 20
	fmt.Printf("candidate=%d start electing leader\n", rf.me)
	for {
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				var args RequestVoteArgs
				server := i
				args.Term = rf.currentTerm
				args.CandidateId = rf.me
				var reply RequestVoteReply
				printTime()
				fmt.Printf("candidate=%d send request vote to server=%d\n", rf.me, i)
				go rf.sendRequestVoteAndTrigger(server, args, &reply, rpcTimeout)
			}
		}
		done = 0
		triggerHeartbeat = false
		for i := 0; i < len(rf.peers)-1; i++ {
			printTime()
			fmt.Printf("candidate=%d waiting for select for i=%d\n", rf.me, i)
			select {
			case ok := <-rf.electCh:
				if ok {
					done++
					success = done >= len(rf.peers)/2 || rf.currentLeader > -1
					success = success && rf.votedFor == rf.me
					if success && !triggerHeartbeat {
						triggerHeartbeat = true
						rf.mu.Lock()
						rf.becomeLeader()
						rf.mu.Unlock()
						rf.heartbeat <- true
						printTime()
						fmt.Printf("candidate=%d becomes leader\n", rf.currentLeader)
					}
				}
			}
			printTime()
			fmt.Printf("candidate=%d complete for select for i=%d\n", rf.me, i)
		}
		if (timeout+last < milliseconds()) || (done >= len(rf.peers)/2 || rf.currentLeader > -1) {
			break
		} else {
			select {
			case <-time.After(time.Duration(10) * time.Millisecond):
			}
		}
	}
	printTime()
	fmt.Printf("candidate=%d receive votes status=%t\n", rf.me, success)
	return success
}

func (rf *Raft) election() {
	var result bool
	for {
		timeout := randomRange(150, 300)
		printTime()
		fmt.Printf("candidate=%d wait election timeout=%d\n", rf.me, timeout)
		rf.setMessageTime(milliseconds())
		for rf.lastMessageTime+timeout > milliseconds() {
			select {
			case <-time.After(time.Duration(timeout) * time.Millisecond):
				printTime()
				fmt.Printf("candidate=%d, lastMessageTime=%d, timeout=%d, plus=%d, now=%d\n", rf.me, rf.lastMessageTime, timeout, rf.lastMessageTime+timeout, milliseconds())
				if rf.lastMessageTime+timeout <= milliseconds() {
					break
				} else {
					rf.setMessageTime(milliseconds())
					timeout = randomRange(150, 300)
					continue
				}
			}
		}

		printTime()
		fmt.Printf("candidate=%d timeouted\n", rf.me)
		// election till success
		result = false
		for !result {
			result = rf.election_one_round()
		}
	}
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
	rf.setTerm(0)
	rf.votedFor = -1
	rf.state = 0 // 0 is follower | 1 is candidate | 2 is leader
	rf.timeout = 0
	rf.currentLeader = -1
	rf.electCh = make(chan bool)
	rf.message = make(chan bool)
	rf.heartbeat = make(chan bool)
	rf.heartbeatRe = make(chan bool)
	rand.Seed(time.Now().UnixNano())

	// Your initialization code here.
	go rf.election()
	go rf.sendLeaderHeartBeat()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
