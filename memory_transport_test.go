package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// inMemoryTransport routes RPCs directly to registered Raft nodes in-process.
type inMemoryTransport struct {
	mu    sync.Mutex
	nodes map[string]*Raft
}

func newInMemoryTransport() *inMemoryTransport {
	return &inMemoryTransport{
		nodes: make(map[string]*Raft),
	}
}

func (t *inMemoryTransport) register(node *Raft) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[node.id] = node
}

func (t *inMemoryTransport) RequestVote(peerID string, req RequestVoteRequest) (RequestVoteResponse, error) {
	t.mu.Lock()
	peer, ok := t.nodes[peerID]
	t.mu.Unlock()

	if !ok {
		return RequestVoteResponse{}, fmt.Errorf("unknown peer %q", peerID)
	}

	return peer.RequestVote(req), nil
}

func (t *inMemoryTransport) AppendEntries(peerID string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	t.mu.Lock()
	peer, ok := t.nodes[peerID]
	t.mu.Unlock()

	if !ok {
		return AppendEntriesResponse{}, fmt.Errorf("unknown peer %q", peerID)
	}

	return peer.AppendEntries(req), nil
}

func newTestCluster(t *testing.T, ids ...string) ([]*Raft, *inMemoryTransport) {
	t.Helper()

	transport := newInMemoryTransport()
	nodes := make([]*Raft, 0, len(ids))

	for _, id := range ids {
		peers := make([]string, 0, len(ids)-1)
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}

		node := NewRaft(id, peers, transport)
		node.electionTimeoutMin = 50 * time.Millisecond
		node.electionTimeoutMax = 50 * time.Millisecond

		transport.register(node)
		nodes = append(nodes, node)
	}

	return nodes, transport
}

func countLeaders(nodes []*Raft) int {
	count := 0
	for _, node := range nodes {
		node.mu.Lock()
		if node.volatile.role == Leader {
			count++
		}
		node.mu.Unlock()
	}
	return count
}

func stopCluster(nodes []*Raft) {
	for _, node := range nodes {
		node.Stop()
	}
}

func TestElectionWithInMemoryTransport(t *testing.T) {
	nodes, _ := newTestCluster(t, "node-a", "node-b", "node-c")

	for _, node := range nodes {
		node.Start()
	}
	defer stopCluster(nodes)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if countLeaders(nodes) == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("expected exactly one leader, got %d", countLeaders(nodes))
}
