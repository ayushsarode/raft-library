package raft

import "sync"

type Raft struct {
	mu sync.Mutex

	id    string
	peers []string

	persisted persistedState
	volatile  volatileState
}

func NewRaft(id string, peers []string) *Raft {
	return &Raft{
		id:    id,
		peers: peers,
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
	}

}

func (r *Raft) RequestVote(req RequestVoteRequest) RequestVoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	if req.Term < r.persisted.currentTerm {
		return RequestVoteResponse{
			Term:        r.persisted.currentTerm,
			VoteGranted: false,
		}
	}

	if req.Term > r.persisted.currentTerm {
		r.persisted.currentTerm = req.Term
		r.persisted.votedFor = ""
		r.volatile.role = Follower
	}

	canVote := r.persisted.votedFor == "" || r.persisted.votedFor == req.CandidateId
	if canVote && r.isCandidateLogUpToDate(req.LastLogIndex, req.LastLogTerm) {
		r.persisted.votedFor = req.CandidateId

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

func (r *Raft) isCandidateLogUpToDate(candidateLastIndex uint64, candidateLastTerm uint64) bool {
	localLastIndex, localLastTerm := r.lastLogPosition()

	if candidateLastTerm != localLastTerm {
		return candidateLastTerm > localLastTerm
	}

	return candidateLastIndex >= localLastIndex
}

func (r *Raft) lastLogPosition() (uint64, uint64) {
	if len(r.persisted.log) == 0 {
		return 0, 0
	}

	last := r.persisted.log[len(r.persisted.log)-1]
	return last.Index, last.Term
}

func (r *Raft) AppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	if req.Term < r.persisted.currentTerm {
		return AppendEntriesResponse{
			Term:    r.persisted.currentTerm,
			Success: false,
		}
	}

	if req.Term > r.persisted.currentTerm {
		r.persisted.currentTerm = req.Term
		r.persisted.votedFor = ""
	}
	r.volatile.role = Follower

	return AppendEntriesResponse{
		Term:    r.persisted.currentTerm,
		Success: true,
	}
}
