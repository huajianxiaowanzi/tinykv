# AGENTS.md

## Build & Test Commands

```bash
make              # Build binaries to bin/
make proto        # Generate protobuf Go code
make test         # Run all tests (native mode)
```

### Single Test (Recommended Pattern)
```bash
go test -v --count=1 --parallel=1 -p=1 ./path/to/package -run TestName
```
- `--parallel=1 -p=1` required for raftstore tests (cleans /tmp/*test-raftstore* between runs)

### Project-Specific Tests
```bash
go test -v --count=1 --parallel=1 -p=1 ./kv/server -run "1$"         # Project 1
go test -v --count=1 --parallel=1 -p=1 ./raft -run "2A$"            # Project 2A
go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run "2B$"  # Project 2B
go test -v --count=1 --parallel=1 -p=1 ./raft -run "2C$"             # Project 2C
go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run "2C$" # Project 2C
go test -v --count=1 --parallel=1 -p=1 ./raft -run "3A$"            # Project 3A
go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run "3B$"  # Project 3B
go test -v --count=1 --parallel=1 -p=1 ./scheduler/server -check.f="3C"  # Project 3C
go test -v --count=1 --parallel=1 -p=1 ./kv/transaction/... -run "4A$" # Project 4A
go test -v --count=1 --parallel=1 -p=1 ./kv/transaction/... -run "4B$" # Project 4B
go test -v --count=1 --parallel=1 -p=1 ./kv/transaction/... -run "4C$" # Project 4C
```

## Directory Ownership

- `kv/` - KV store (server, storage, raftstore, transaction)
- `raft/` - Raft consensus algorithm
- `scheduler/` - Cluster management and timestamp allocation
- `proto/` - Protocol Buffers definitions and generated code

## Proto Generation
Run `make proto` after modifying `.proto` files. Generated code lives in `proto/pkg/`.

## Key Conventions

- Set `LOG_LEVEL=debug` for debugging
- Always call `Discard()` on badger.Txn and close iterators
- Tests use mock network with configurable partitions and reliability
- Badger fork: `github.com/Connor1996/badger` (not dgraph-io/badger)

## Critical Design Patterns

**Follower Also Applies**: Unlike standard Raft (Leader-only apply), TinyKV has ALL nodes apply committed entries. This enables fast failover and local reads. Inherited from TiKV production design.

**Proposals List**: Request matching uses `index+term` to find callbacks during Apply. Critical for understanding the propose→ready→apply→callback flow.

**Region/Peer/Store**: Region = Raft group, Peer = Raft node in a store, Store = Server instance.

## Code Flow (for debugging)

1. Raw API: Client → Server.RawXxx → Storage.Write/Reader
2. Raft API: Client → RaftStorage → raftWorker → propose → ready → apply → callback
3. Transaction API: Client → MvccTxn → underlying Storage with MVCC encoding

## Known Issues

**Windows**: Snapshot file rename fails with "file is being used by another process". Causes test failures in ConfChange scenarios. Run tests in WSL/Linux if possible.

## References

- Deep-dive architecture and detailed flow diagrams: see `./CLAUDE.md`
- Course projects: see `./doc/project*-*.md`