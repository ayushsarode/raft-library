package raft

import (
	"math/rand"
	"sync"
	"time"
)

type Raft struct {
	mu sync.Mutex

	id    string
	peers []string

	transport Transport

	persisted persistedState
	volatile  volatileState

	electionTimeoutMin   time.Duration
	electionTimeoutMax   time.Duration
	resetElectionTimerCh chan struct{}

	stopCh  chan struct{}
	running bool
}

// creates the node state and returns a pointer to the Raft struct
// NewRaft is the constructor for Raft.
// NewRaft initializes a Raft node.
func NewRaft(id string, peers []string, transport Transport) *Raft {
	return &Raft{
		id:        id,
		peers:     peers,
		transport: transport,
		persisted: persistedState{
			currentTerm: 0,
			votedFor:    "",
			log:         []LogEntry{},
		},
		volatile: volatileState{
			role:        Follower,
			commitIndex: 0,
			lastApplied: 0,
			nextIndex:   make(map[string]uint64),
			matchIndex:  make(map[string]uint64),
		},
		electionTimeoutMin:   150 * time.Millisecond,
		electionTimeoutMax:   300 * time.Millisecond,
		resetElectionTimerCh: make(chan struct{}, 1),
	}
}

// starts the node and begins the election process
func (r *Raft) Start() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.running {
		return
	}

	r.stopCh = make(chan struct{})
	r.running = true

	go r.runElectionTimer()
}

func (r *Raft) runElectionTimer() {
	timer := time.NewTimer(r.randomElectionTimeout())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			r.mu.Lock()
			isLeader := r.volatile.role == Leader
			r.mu.Unlock()

			if !isLeader {
				r.startElection()
			}

			timer.Reset(r.randomElectionTimeout())

		case <-r.resetElectionTimerCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}

			timer.Reset(r.randomElectionTimeout())

		case <-r.stopCh:
			return
		}
	}
}

func (r *Raft) randomElectionTimeout() time.Duration {
	delta := r.electionTimeoutMax - r.electionTimeoutMin

	// handle invalid/same values, suppose max and min both are 150ms = 0
	if delta <= 0 {
		return r.electionTimeoutMin
	}

	return r.electionTimeoutMin + time.Duration(rand.Int63n(int64(delta)))
}

func (r *Raft) resetElectionTimer() {
	select {
	case r.resetElectionTimerCh <- struct{}{}:
	default:
	}
}

// stops the node and cleans up resources
func (r *Raft) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.running {
		return
	}

	close(r.stopCh)
	r.running = false
}

func (r *Raft) startElection() {
	r.mu.Lock()

	r.volatile.role = Candidate
	r.persisted.currentTerm++
	r.persisted.votedFor = r.id

	term := r.persisted.currentTerm
	lastLogIndex, lastLogTerm := r.lastLogPosition()
	votes := 1
	majority := r.majority()

	r.mu.Unlock()

	if votes >= majority {
		r.becomeLeader()
		return
	}

	for _, peerID := range r.peers {
		resp, err := r.transport.RequestVote(peerID, RequestVoteRequest{
			Term:         term,
			CandidateId:  r.id,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		})
		if err != nil {
			continue
		}

		r.mu.Lock()

		if r.volatile.role != Candidate || r.persisted.currentTerm != term {
			r.mu.Unlock()
			return
		}

		if resp.Term > r.persisted.currentTerm {
			r.persisted.currentTerm = resp.Term
			r.persisted.votedFor = ""
			r.volatile.role = Follower
			r.mu.Unlock()
			return
		}


		if resp.VoteGranted {
			votes++
			if votes >= majority {
			r.volatile.role = Leader
			r.mu.Unlock()
			return
		}
	}
	r.mu.Unlock()
}
}

// handles the request vote RPC from a candidate
func (r *Raft) RequestVote(req RequestVoteRequest) RequestVoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	// if the candidate's term is less than the current node's term, reject the vote
	if req.Term < r.persisted.currentTerm {
		return RequestVoteResponse{
			Term:        r.persisted.currentTerm,
			VoteGranted: false,
		}
	}

	// if the candidate's term is greater than the current node's term, update the current node's term and reset the vote
	if req.Term > r.persisted.currentTerm {
		r.persisted.currentTerm = req.Term
		r.persisted.votedFor = ""
		r.volatile.role = Follower
	}

	// check if the node has already voted for a candidate in this term
	canVote := r.persisted.votedFor == "" || r.persisted.votedFor == req.CandidateId
	if canVote && r.isCandidateLogUpToDate(req.LastLogIndex, req.LastLogTerm) {
		r.persisted.votedFor = req.CandidateId
		r.resetElectionTimer()

		return RequestVoteResponse{
			Term:        r.persisted.currentTerm,
			VoteGranted: true,
		}
	}

	return RequestVoteResponse{
		Term:        r.persisted.currentTerm,
		VoteGranted: false,
	}
}

// checks if the candidate's log is up to date
func (r *Raft) isCandidateLogUpToDate(candidateLastIndex uint64, candidateLastTerm uint64) bool {
	localLastIndex, localLastTerm := r.lastLogPosition()

	if candidateLastTerm != localLastTerm {
		return candidateLastTerm > localLastTerm
	}

	return candidateLastIndex >= localLastIndex
}

// returns the last log position and term
func (r *Raft) lastLogPosition() (uint64, uint64) {
	if len(r.persisted.log) == 0 {
		return 0, 0
	}

	last := r.persisted.log[len(r.persisted.log)-1]
	return last.Index, last.Term
}

// handles the append entries RPC from the leader
func (r *Raft) AppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	// if the leader's term is less than the current node's term, reject the append entries
	if req.Term < r.persisted.currentTerm {
		return AppendEntriesResponse{
			Term:    r.persisted.currentTerm,
			Success: false,
		}
	}

	// if the leader's term is greater than the current node's term, update the current node's term and reset the vote
	if req.Term > r.persisted.currentTerm {
		r.persisted.currentTerm = req.Term
		r.persisted.votedFor = ""
	}
	r.volatile.role = Follower
	r.resetElectionTimer()

	return AppendEntriesResponse{
		Term:    r.persisted.currentTerm,
		Success: true,
	}
}

func (r *Raft) majority() int {
	return (len(r.peers)+1)/2 + 1
}

func (r *Raft) becomeLeader() {
	r.volatile.role = Leader

	lastLogIndex, _ := r.lastLogPosition()
	nextIndex := lastLogIndex + 1

	for _, peerID := range r.peers {
		r.volatile.nextIndex[peerID] = nextIndex
		r.volatile.matchIndex[peerID] = 0
	}
}