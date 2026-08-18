package raft

type Transport interface {
	RequestVote(peerID string, req RequestVoteRequest) (RequestVoteResponse, error)
	AppendEntries(peerID string, req AppendEntriesRequest) (AppendEntriesResponse, error)
}
