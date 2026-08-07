package raft

// role is just state that  changes as a result of election
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)
type LogEntry struct {
	Term uint64
	Index uint64
	Command []byte
}

type persistedState struct {
	currentTerm uint64
	votedFor string
	log []LogEntry 
}

type volatileState struct {
	role Role
	commitIndex uint64
	lastApplied uint64	

	// leader only, reset on election
	nextIndex map[string]uint64
	matchIndex map[string]uint64
}

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	return "unknown"
}
