package raft

// please Use 3.2 raft for testing of Pa3.2
//this raft is for pa4

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
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for Assignment2; only used in Assignment3
	Snapshot    []byte // ignore for Assignment2; only used in Assignment3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                  sync.Mutex
	peers               []*labrpc.ClientEnd
	persister           *Persister
	me                  int // index into peers[]
	currentTerm         int
	state               string
	voteSent            chan int //tells election timer to reset
	appendEntryReceived chan int //tells election timer to reset

	votesReceivedInTerm map[int]string // set of nodes who voted in a term
	applychan           chan ApplyMsg
	votedFor            int         //id of cand who got vote in this term
	leaderId            int         //leader of curr term
	log                 []Log_entry //log
	commitIndex         int         //points at last commited val
	lastApplied         int         //all preceding entries applied before this point
	matchIndex          []int       //highest match with each server
	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == "leader" {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
	// Your code here.
	return term, isleader
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	Term        int //send cand term
	CandidateId int //send cand id
	Log         []Log_entry
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int  // send respondent term
	VoterId     int  // send respondent id to track who voted
	VoteGranted bool // true if granted
}
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int         //leader send his id
	Enteries     []Log_entry //leader log
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int  //respondent term
	Success bool // true when append entry accepted means from valid leader
}
type Log_entry struct {
	Term    int
	Command interface{}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) { //handle vote req from cand
	// Your code here.

	rf.mu.Lock() //store variables to be used
	store_term := rf.currentTerm
	store_state := rf.state
	store_new_voterid := rf.me
	store_votedFor := rf.votedFor
	my_log := rf.log
	rf.mu.Unlock()
	if args.Term < store_term { // can be in partition

		reply.Term = store_term
		reply.VoteGranted = false
		reply.VoterId = store_new_voterid
	} else if args.Term > store_term {
		rf.mu.Lock()
		rf.currentTerm = args.Term // or arg.term

		rf.votedFor = -1
		rf.votesReceivedInTerm = map[int]string{}
		rf.state = "follower"

		store_new_term := rf.currentTerm

		rf.mu.Unlock()
		len_my_log := len(my_log)
		len_arg_log := len(args.Log)
		if (len_my_log == 0 && len_arg_log == 0) || (len_my_log == 0) { //initial case when logs zero
			rf.mu.Lock()

			rf.votedFor = args.CandidateId

			reply.VoteGranted = true
			rf.mu.Unlock()
		} else if len_my_log > 0 && len_arg_log == 0 {
			reply.VoteGranted = false
		} else if (my_log[len_my_log-1].Term < args.Log[len_arg_log-1].Term) || ((my_log[len_my_log-1].Term == args.Log[len_arg_log-1].Term) && (len_my_log <= len_arg_log)) {
			rf.mu.Lock()

			rf.votedFor = args.CandidateId

			reply.VoteGranted = true
			rf.mu.Unlock()
		} else {
			reply.VoteGranted = false
		}

		reply.Term = store_new_term
		reply.VoterId = store_new_voterid

		rf.voteSent <- 1 //when vote casted reset timer

	} else { //when both term equal probably candidate asking cand for vote, or they voted for some other cand that is why their term update,

		if store_state == "candidate" {
			reply.VoteGranted = false
			reply.Term = store_term
			reply.VoterId = store_new_voterid
		}
		if store_state == "follower" {
			if store_votedFor == -1 {

				len_my_log := len(my_log)
				len_arg_log := len(args.Log)
				if (len_my_log == 0 && len_arg_log == 0) || (len_my_log == 0) {
					rf.mu.Lock()

					rf.votedFor = args.CandidateId

					reply.VoteGranted = true
					rf.mu.Unlock()
				} else if len_my_log > 0 && len_arg_log == 0 {
					reply.VoteGranted = false
				} else if (my_log[len_my_log-1].Term < args.Log[len_arg_log-1].Term) || ((my_log[len_my_log-1].Term == args.Log[len_arg_log-1].Term) && (len_my_log <= len_arg_log)) {
					rf.mu.Lock()

					rf.votedFor = args.CandidateId

					reply.VoteGranted = true
					rf.mu.Unlock()
				} else {
					reply.VoteGranted = false
				}
				reply.Term = store_term
				reply.VoterId = store_new_voterid

				rf.voteSent <- 1
			} else {
				reply.VoteGranted = false
				reply.Term = store_term
				reply.VoterId = store_new_voterid

			}
		}
	}
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) { //node receive it from leader
	rf.mu.Lock()
	store_state := rf.state
	store_term := rf.currentTerm
	my_log := rf.log
	len_my_log := len(my_log)
	rf.mu.Unlock()

	if args.Term < store_term { // can be in partitioned (leader)
		reply.Term = store_term
		reply.Success = false
	}

	if args.Term >= store_term { //append entry from valid leader

		append_check := rf.appendEntriesCheck(args.Enteries, len(args.Enteries), my_log, len_my_log)

		if append_check {

			if store_state == "candidate" {
				rf.mu.Lock()
				rf.state = "follower"
				rf.currentTerm = args.Term
				rf.leaderId = args.LeaderId
				rf.votedFor = -1
				rf.votesReceivedInTerm = map[int]string{}
				store_new_term := rf.currentTerm

				rf.mu.Unlock()

				rf.appendEntryReceived <- 1 //reset election timer

				rf.replicateLog(args.Enteries, args.LeaderCommit) //rep log
				reply.Term = store_new_term
				reply.Success = true

			} else if store_state == "leader" {
				rf.mu.Lock()
				rf.state = "follower"
				rf.currentTerm = args.Term
				rf.leaderId = args.LeaderId
				rf.votedFor = -1
				store_new_term := rf.currentTerm

				rf.mu.Unlock()

				rf.replicateLog(args.Enteries, args.LeaderCommit)
				rf.appendEntryReceived <- 1
				reply.Term = store_new_term
				reply.Success = true
			} else {
				rf.mu.Lock()
				rf.currentTerm = args.Term
				rf.leaderId = args.LeaderId
				store_new_term := rf.currentTerm
				rf.votedFor = -1

				rf.mu.Unlock()
				rf.appendEntryReceived <- 1

				rf.replicateLog(args.Enteries, args.LeaderCommit)
				reply.Term = store_new_term
				reply.Success = true

			}
		} else {
			reply.Term = store_term
			reply.Success = false
		}
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool { //makes rpc call for reqvote
	rf.mu.Lock()
	store_peers_server := rf.peers[server]
	rf.mu.Unlock()
	ok := store_peers_server.Call("Raft.RequestVote", args, reply)

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool { //make rpc call for append entry
	rf.mu.Lock()
	store_peers_server := rf.peers[server]
	rf.mu.Unlock()
	ok := store_peers_server.Call("Raft.AppendEntries", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == "leader" {
		isLeader = true
		term = rf.currentTerm
		entry := Log_entry{Term: term, Command: command}
		rf.log = append(rf.log, entry)

		index = len(rf.log)
		return index, term, isLeader

	} else {
		isLeader = false
		return index, term, isLeader
	}

}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
	//rf.killAppendEntryTimer <- 1
	//rf.killElectionTimer <- 1
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
func Make(peers []*labrpc.ClientEnd, me int, // initialize stuff and election timer routine
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.state = "follower"
	rf.votesReceivedInTerm = map[int]string{}
	rf.log = make([]Log_entry, 0)
	rf.matchIndex = make([]int, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.leaderId = -1
	rf.applychan = applyCh
	rf.voteSent = make(chan int)
	rf.appendEntryReceived = make(chan int)

	for i := 0; i < len(peers); i++ {
		rf.matchIndex = append(rf.matchIndex, -1)
	}

	go rf.electionTimerRoutine()
	go rf.appendEntriesTimer()

	return rf
}

func (rf *Raft) sendRequestVoteRoutine(server int) { //called for each to whom rpc sent and reply received
	rf.mu.Lock()
	store_term := rf.currentTerm
	store_id := rf.me
	store_log := rf.log
	rf.mu.Unlock()
	args := RequestVoteArgs{Term: store_term, CandidateId: store_id, Log: store_log}
	reply := new(RequestVoteReply)

	ok := rf.sendRequestVote(server, args, reply)

	if ok {
		//lock
		rf.mu.Lock()
		store_term := rf.currentTerm
		store_state := rf.state

		rf.mu.Unlock()
		if reply.Term > store_term {
			rf.voteSent <- 1
			rf.mu.Lock()
			rf.state = "follower"
			rf.currentTerm = reply.Term
			rf.votesReceivedInTerm = map[int]string{}
			rf.votedFor = -1

			rf.mu.Unlock()

		} else if store_state == "candidate" && reply.Term == store_term && reply.VoteGranted { //leader getting elected
			rf.voteSent <- 1
			rf.mu.Lock()
			rf.votesReceivedInTerm[reply.VoterId] = ""
			store_len := len(rf.votesReceivedInTerm)
			rf.mu.Unlock()
			if store_len > len(rf.peers)/2 {
				rf.mu.Lock()
				rf.state = "leader"
				rf.votesReceivedInTerm = map[int]string{}
				rf.votedFor = -1
				rf.leaderId = rf.me

				rf.mu.Unlock()

			}

		}
	}

}

func (rf *Raft) sendAppendEntriesRoutine(server int) { //handle append entry of each server
	rf.mu.Lock()
	store_term := rf.currentTerm
	store_id := rf.me
	store_log := rf.log
	store_commitIndex := rf.commitIndex
	rf.mu.Unlock()
	args := AppendEntriesArgs{Term: store_term, LeaderId: store_id, Enteries: store_log, LeaderCommit: store_commitIndex}
	reply := new(AppendEntriesReply)

	ok := rf.sendAppendEntries(server, args, reply)
	if ok {
		rf.mu.Lock()
		store_term := rf.currentTerm
		rf.mu.Unlock()
		if reply.Term > store_term || !reply.Success {

			rf.mu.Lock()
			rf.currentTerm = reply.Term
			rf.state = "follower"
			rf.votedFor = -1
			rf.leaderId = -1
			rf.votesReceivedInTerm = map[int]string{}

			rf.mu.Unlock()

		} else {
			if reply.Success {
				rf.mu.Lock()
				rf.matchIndex[server] = len(store_log) - 1
				store_matchIndex := rf.matchIndex
				store_log = rf.log
				store_commitIndex = rf.commitIndex
				rf.mu.Unlock()
				for i := len(store_log) - 1; i > store_commitIndex; i-- { //update commit index if by taking majority of match index
					majority_entry := 1

					for j := 0; j < len(store_matchIndex); j++ {
						rf.mu.Lock()
						if store_matchIndex[j] >= i {
							majority_entry += 1
						}
						rf.mu.Unlock()
					}

					if majority_entry > len(store_matchIndex)/2 {
						rf.mu.Lock()
						if rf.log[i].Term == rf.currentTerm {
							rf.commitIndex = i
						}

						rf.mu.Unlock()
						break
					}
				}

				rf.mu.Lock()
				store_commitIndex = rf.commitIndex
				store_lastApplied := rf.lastApplied

				if store_lastApplied <= store_commitIndex && len(store_log) > 0 {
					for i := store_lastApplied; i <= store_commitIndex; i++ {
						apply_msg := ApplyMsg{Index: i + 1, Command: store_log[i].Command}
						rf.lastApplied = i + 1
						rf.applychan <- apply_msg
					}

				}
				rf.mu.Unlock()

			}
		}

	}
}
func (rf *Raft) electionTimerRoutine() {
	for {

		select {
		case <-rf.appendEntryReceived:

		case <-rf.voteSent:

		case <-time.After(time.Duration((rand.Intn(150) + 300)) * time.Millisecond):
			//locks
			rf.mu.Lock()
			store_state := rf.state
			rf.mu.Unlock()
			if store_state == "leader" {
				continue
			}
			rf.mu.Lock()
			rf.currentTerm += 1

			rf.state = "candidate"
			rf.votedFor = rf.me
			rf.votesReceivedInTerm[rf.me] = ""

			rf.mu.Unlock()

			for k, _ := range rf.peers {
				if k != rf.me {
					go rf.sendRequestVoteRoutine(k)
				}
			}

		}

	}
}

func (rf *Raft) appendEntriesTimer() {
	for {

		select {
		case <-time.After(time.Duration(60) * time.Millisecond):
			rf.mu.Lock()
			store_state := rf.state
			rf.mu.Unlock()
			if store_state == "leader" {
				for k, _ := range rf.peers {
					if k != rf.me {
						go rf.sendAppendEntriesRoutine(k)
					}
				}
			}

		}

	}
}

func (rf *Raft) replicateLog(leader_log []Log_entry, leader_commit int) { //re[licate log on server]
	rf.mu.Lock()
	rf.commitIndex = leader_commit
	store_commitIndex := leader_commit
	rf.log = leader_log

	store_lastApplied := rf.lastApplied
	store_log := rf.log

	if store_lastApplied <= store_commitIndex && len(store_log) > 0 {
		for i := store_lastApplied; i <= store_commitIndex; i++ {
			apply_msg := ApplyMsg{Index: i + 1, Command: store_log[i].Command}
			rf.lastApplied = i + 1
			rf.applychan <- apply_msg
		}

	}
	rf.mu.Unlock()

}

func (rf *Raft) appendEntriesCheck(arg_log []Log_entry, len_arg_log int, my_log []Log_entry, len_my_log int) bool {
	if (len_my_log == 0) || (len_my_log == 0 && len_arg_log == 0) {
		return true
	} else if len_my_log > 0 && len_arg_log == 0 {
		return false
	} else if (arg_log[len_arg_log-1].Term >= my_log[len_my_log-1].Term) || ((arg_log[len_arg_log-1].Term == my_log[len_my_log-1].Term) && len_arg_log >= len_my_log) {
		return true
	} else {
		return false
	}
}
