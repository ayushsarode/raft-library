# Raft Library

A standalone Go implementation of the Raft consensus protocol, starting with leader election for a distributed job scheduler.

## Initial Scope

The first milestone is intentionally small:

- start a cluster of Raft nodes
- elect exactly one leader
- maintain leadership with heartbeats
- elect a new leader when the current leader stops

Log replication and command application will come after leader election is stable.

## Package Layout

```text
.
├── state.go
├── statemachine.go
├── raft.go
├── rpc.go
├── rpc_handlers.go
├── transport.go
├── fake_transport_test.go
├── raft_test.go
└── cmd/example
```

The package is kept flat at first to avoid premature package boundaries and Go import cycles.
