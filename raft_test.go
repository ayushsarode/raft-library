package raft

import "testing"

func TestRequestVoteGrantsVoteWhenNotVoted(t *testing.T) {
	r := NewRaft("node-a", []string{"node-b", "node-c"})

	resp := r.RequestVote(RequestVoteRequest{
		Term:         1,
		CandidateId:  "node-b",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !resp.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	if resp.Term != 1 {
		t.Fatalf("expected term 1, got %d", resp.Term)
	}

	if r.persisted.votedFor != "node-b" {
		t.Fatalf("expected votedFor node-b, got %q", r.persisted.votedFor)
	}
}

func TestRequestVoteRejectsStaleTerm(t *testing.T) {
	r := NewRaft("node-a", nil)
	r.persisted.currentTerm = 3

	resp := r.RequestVote(RequestVoteRequest{
		Term:         2,
		CandidateId:  "node-b",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if resp.VoteGranted {
		t.Fatal("expected vote to be rejected")
	}

	if resp.Term != 3 {
		t.Fatalf("expected response term 3, got %d", resp.Term)
	}
}

func TestRequestVoteUpdatesTermAndStepsDownOnNewerTerm(t *testing.T) {
	r := NewRaft("node-a", nil)
	r.persisted.currentTerm = 2
	r.persisted.votedFor = "node-c"
	r.volatile.role = Leader

	resp := r.RequestVote(RequestVoteRequest{
		Term:         3,
		CandidateId:  "node-b",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !resp.VoteGranted {
		t.Fatal("expected vote to be granted")
	}

	if r.persisted.currentTerm != 3 {
		t.Fatalf("expected current term 3, got %d", r.persisted.currentTerm)
	}

	if r.volatile.role != Follower {
		t.Fatalf("expected role Follower, got %s", r.volatile.role)
	}

	if r.persisted.votedFor != "node-b" {
		t.Fatalf("expected votedFor node-b, got %q", r.persisted.votedFor)
	}
}

func TestRequestVoteRejectsSecondCandidateInSameTerm(t *testing.T) {
	r := NewRaft("node-a", nil)
	r.persisted.currentTerm = 1
	r.persisted.votedFor = "node-b"

	resp := r.RequestVote(RequestVoteRequest{
		Term:         1,
		CandidateId:  "node-c",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if resp.VoteGranted {
		t.Fatal("expected vote to be rejected")
	}

	if r.persisted.votedFor != "node-b" {
		t.Fatalf("expected votedFor to remain node-b, got %q", r.persisted.votedFor)
	}
}

func TestRequestVoteAllowsRepeatedVoteForSameCandidate(t *testing.T) {
	r := NewRaft("node-a", nil)
	r.persisted.currentTerm = 1
	r.persisted.votedFor = "node-b"

	resp := r.RequestVote(RequestVoteRequest{
		Term:         1,
		CandidateId:  "node-b",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	if !resp.VoteGranted {
		t.Fatal("expected repeated vote for same candidate to be granted")
	}
}

func TestRequestVoteRejectsStaleCandidateLogTerm(t *testing.T) {
	r := NewRaft("node-a", nil)
	r.persisted.log = []LogEntry{
		{Term: 1, Index: 1},
		{Term: 2, Index: 2},
	}

	resp := r.RequestVote(RequestVoteRequest{
		Term:         3,
		CandidateId:  "node-b",
		LastLogIndex: 10,
		LastLogTerm:  1,
	})

	if resp.VoteGranted {
		t.Fatal("expected vote to be rejected for stale candidate log term")
	}
}

func TestRequestVoteRejectsStaleCandidateLogIndex(t *testing.T) {
	r := NewRaft("node-a", nil)
	r.persisted.log = []LogEntry{
		{Term: 2, Index: 1},
		{Term: 2, Index: 2},
	}

	resp := r.RequestVote(RequestVoteRequest{
		Term:         3,
		CandidateId:  "node-b",
		LastLogIndex: 1,
		LastLogTerm:  2,
	})

	if resp.VoteGranted {
		t.Fatal("expected vote to be rejected for stale candidate log index")
	}
}

func TestRequestVoteGrantsVoteForMoreRecentCandidateLog(t *testing.T) {
	r := NewRaft("node-a", nil)
	r.persisted.log = []LogEntry{
		{Term: 1, Index: 1},
		{Term: 2, Index: 2},
	}

	resp := r.RequestVote(RequestVoteRequest{
		Term:         3,
		CandidateId:  "node-b",
		LastLogIndex: 2,
		LastLogTerm:  3,
	})

	if !resp.VoteGranted {
		t.Fatal("expected vote to be granted for more recent candidate log")
	}
}

func TestAppendEntriesRejectionForStaleTerm(t *testing.T) {
	r := NewRaft("node-a", nil)
	r.persisted.currentTerm = 3

	resp := r.AppendEntries(AppendEntriesRequest{
		Term:         2,
		LeaderId:     "node-b",
	})

	if resp.Success {
		t.Fatal("expected append entries to be rejected for stale term")
	}

	if resp.Term != 3 {
		t.Fatalf("expected response term 3, got %d", resp.Term)
	}
}

func TestAppendEntriesAcceptsCurrentTermHeartbeat(t *testing.T) {
	r := NewRaft("node-a", nil)
	r.persisted.currentTerm = 3
	r.volatile.role = Candidate

	resp := r.AppendEntries(AppendEntriesRequest{
		Term:     3,
		LeaderId: "node-b",
	})

	if !resp.Success {
		t.Fatal("expected append entries to be accepted")
	}

	if resp.Term != 3 {
		t.Fatalf("expected term 3, got %d", resp.Term)
	}

	if r.volatile.role != Follower {
		t.Fatalf("expected role Follower, got %s", r.volatile.role)
	}
}

func TestAppendEntriesUpdatesTermAndClearsVoteOnNewerTerm(t *testing.T) {
	r := NewRaft("node-a", nil)
	r.persisted.currentTerm = 2
	r.persisted.votedFor = "node-c"
	r.volatile.role = Leader

	resp := r.AppendEntries(AppendEntriesRequest{
		Term:     3,
		LeaderId: "node-b",
	})

	if !resp.Success {
		t.Fatal("expected append entries to be accepted")
	}

	if resp.Term != 3 {
		t.Fatalf("expected response term 3, got %d", resp.Term)
	}

	if r.persisted.currentTerm != 3 {
		t.Fatalf("expected current term 3, got %d", r.persisted.currentTerm)
	}

	if r.persisted.votedFor != "" {
		t.Fatalf("expected votedFor to be cleared, got %q", r.persisted.votedFor)
	}

	if r.volatile.role != Follower {
		t.Fatalf("expected role Follower, got %s", r.volatile.role)
	}
}