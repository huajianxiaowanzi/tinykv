# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
make              # Build tinykv-server and tinyscheduler-server binaries
make proto        # Generate Protocol Buffers Go code
make test         # Run all tests
make project1     # Test standalone KV storage
make project2a    # Test Raft implementation (2AA, 2AB, 2AC for sub-parts)
make project2b    # Test fault-tolerant KV on Raft
make project2c    # Test snapshot and log GC
make project3a    # Test leader transfer and conf change
make project3b    # Test region split and conf change in raftstore
make project3c    # Test scheduler balance
make project4a    # Test MVCC transaction layer
make project4b    # Test KvGet/KvPrewrite/KvCommit
make project4c    # Test KvScan/KvCheckTxnStatus/KvBatchRollback/KvResolveLock
```

## Future: TinyKV Operator Plan

After completing core TinyKV functionality, implement a Kubernetes Operator for automated deployment and operations:

### Planned CRDs
- `TinyKVCluster` - Main cluster resource (Scheduler, Store, Region configuration)
- `TinyKVBackup` - Scheduled backup to S3/NFS
- `TinyKVRestore` - Restore from backup
- `TinyKVScale` - Auto-scaling based on region count/metrics

### Core Features
- Automatic Store failover and recovery
- Graceful scale-in with Region migration
- TLS certificate rotation
- PVC rebinding for rebuilt Pods
- Integration with Scheduler API for balance operations

### Tech Stack
- Operator-SDK (Go)
- controller-runtime
- Helm Chart for packaging

Run single test: `go test -v ./path/to/package -run TestName`

## Windows 11 Test Commands (without make)

### Run All Tests for Each Project

```powershell
# Project 1 - Standalone KV Storage
go test -v --count=1 --parallel=1 -p=1 ./kv/server -run "1$"

# Project 2A - Raft (2AA, 2AB, 2AC)
go test -v --count=1 --parallel=1 -p=1 ./raft -run "2A$"

# Project 2B - Fault-tolerant KV on Raft
go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run "2B$"

# Project 2C - Snapshot and Log GC
go test -v --count=1 --parallel=1 -p=1 ./raft -run "2C$"
go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run "2C$"

# Project 3A - Raft Leader Transfer and Conf Change
go test -v --count=1 --parallel=1 -p=1 ./raft -run "3A$"

# Project 3B - Region Split and Conf Change in raftstore
go test -v --count=1 --parallel=1 -p=1 ./kv/test_raftstore -run "3B$"

# Project 3C - Scheduler Balance
go test -v --count=1 --parallel=1 -p=1 ./scheduler/server ./scheduler/server/schedulers -check.f="3C"

# Project 4A - MVCC Transaction Layer
go test -v --count=1 --parallel=1 -p=1 ./kv/transaction/... -run "4A$"

# Project 4B - KvGet/KvPrewrite/KvCommit
go test -v --count=1 --parallel=1 -p=1 ./kv/transaction/... -run "4B$"

# Project 4C - KvScan/KvCheckTxnStatus/KvBatchRollback/KvResolveLock
go test -v --count=1 --parallel=1 -p=1 ./kv/transaction/... -run "4C$"
```

## Architecture Overview

TinyKV is a distributed KV storage system inspired by MIT 6.824 and TiKV, with computation/storage separation architecture.

### Core Components

- **kv/** - Key-value store implementation
  - `server/` - gRPC service handlers (Raw API and Transaction API)
  - `storage/` - Storage interface with StandaloneStorage and RaftStorage implementations
  - `raftstore/` - Multi-Raft coordination layer (peer management, ready processing, region workers)
  - `transaction/` - MVCC, 2PC transaction support with latches
  - `util/engine_util/` - Column family simulation via key prefixing on Badger

- **raft/** - Raft consensus algorithm implementation
  - `raft.go` - Core Raft state machine (election, replication)
  - `log.go` - RaftLog helper for log management
  - `rawnode.go` - RawNode interface for upper application interaction
  - `storage.go` - Storage interface for persistence abstraction

- **scheduler/** - Central control for cluster management and timestamp allocation
  - `server/cluster.go` - Cluster metadata and heartbeat processing
  - `server/schedulers/` - Balance region scheduler

- **proto/** - Protocol Buffers definitions over gRPC
  - `proto/` - .proto source files
  - `pkg/` - Generated Go code

### Data Flow

1. **Raw API**: Client RPC → Server.RawGet/Put/Delete/Scan → Storage.Write/Reader
2. **Raft API**: Client RPC → RaftStorage → raftWorker (propose → ready → apply) → callback
3. **Transaction API**: Client RPC → MvccTxn → underlying Storage with MVCC encoding

### Raft Write Path (详细)

```
Client 请求
    ↓
RaftStorage (接收请求，kv/storage/raft_storage/raft_server.go)
    ↓
RaftCmdRequest (打包成工单，proto/proto/raft_cmdpb.proto)
    ↓
raftCh (放入收件箱，channel 发送到 raftWorker)
    ↓
raftWorker (取出工单，kv/raftstore/raft_worker.go)
    ↓
Raft.propose (提交讨论，raft/raft.go)
    ↓
Raft.ready (等待投票结果，通过 Ready 结构返回)
    ↓
Apply (真正写入数据库，kv/raftstore/apply.go)
    ↓
返回结果给客户端
```

### Raft Propose 详细流程 (peerMsgHandler.proposeRequest)

**位置**: `kv/raftstore/peer_msg_handler.go`

```
1. Client: Put("foo", "bar")
           ↓
2. proposeRequest()
   - 保存 proposal (index=6, cb=xxx) 到 d.proposals 列表
   - 序列化 RaftCmdRequest 成字节流
   - 调用 RaftGroup.Propose(data)
           ↓
3. Raft 复制日志给 Follower
   Leader:   [Entry{Index:6, Data:...}]
   Follower: [Entry{Index:6, Data:...}] ✓
           ↓
4. Apply 时查找 proposal
   if entry.Index == proposal.index {
       proposal.cb.Done(response)  ← 返回客户端
   }
```

**proposal 结构**:
```go
type proposal struct {
    index uint64        // 日志在 Raft 中的位置，用于匹配 Apply 的 entry
    term  uint64        // 任期号，用于检测 leader 变更
    cb    *Callback     // 客户端的回调，用于返回结果
}
```

**关键点**:
- Raft 只关心字节流，不关心内容
- proposal 列表用于在 Apply 时找到对应的客户端回调
- Leader 挂掉后，未提交的 proposal 会超时，客户端重试

### 完整回调数据链路 (从 Propose 到 Callback.Done)

```
1. RaftStorage.Write()
   cb := message.NewCallback()  // 创建带 done channel 的回调
   rs.raftRouter.SendRaftCommand(request, cb)
           ↓
2. RaftstoreRouter.SendRaftCommand()
   - 包装成 MsgRaftCmd (含 Request + Callback)
   - 发送到 router.peerSender channel (40960 缓冲)
           ↓
3. raftWorker.run() 从 raftCh 接收
   for msg := range rw.raftCh {
       peerMsgHandler.HandleMsg(msg)
   }
           ↓
4. HandleMsg() case MsgTypeRaftCmd:
   d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
           ↓
5. proposeRaftCommand()
   - preProposeRaftCommand() 检查 (Leader、Term、RegionEpoch)
   - proposeRequest(msg, cb)
           ↓
6. proposeRequest()
   d.proposals = append(d.proposals, &proposal{
       index: d.RaftGroup.Raft.RaftLog.LastIndex() + 1,
       term:  d.RaftGroup.Raft.Term,
       cb:    cb,  // 保存回调
   })
   d.RaftGroup.Propose(data)  // 提交给 Raft
           ↓
7. Raft 内部处理
   - 创建 Entry { Term, Index, Data }
   - 发送 MsgAppend 给 Follower
   - 等待过半数确认 → Committed
           ↓
8. HandleRaftReady() 【需实现】
   rd := d.RaftGroup.Ready()
   d.peerStorage.SaveReadyState(rd)  // 持久化到 raftdb
   send(rd.Messages)                 // 发送 Raft 消息
   applyCommittedEntries(rd.CommittedEntries)  // Apply 日志
           ↓
9. applyCommittedEntries() 【需实现】
   for _, entry := range entries {
       cmd.Unmarshal(entry.Data)
       execWriteRequest(cmd)      // 写入 KV
       onRaftBaseResp(cmd, index) // 查找 proposal 并回调
   }
           ↓
10. onRaftBaseResp() 【需实现】
    for i, p := range d.proposals {
        if p.index == entry.Index && p.term == entry.Term {
            p.cb.Done(resp)  // 触发回调!
            删除 proposal
            break
        }
    }
           ↓
11. Callback.Done()
    cb.Resp = resp
    cb.done <- struct{}{}  // 发送信号，唤醒等待的协程
           ↓
12. RaftStorage.Write() 返回
    cb.WaitResp()  // 从 done channel 接收
    return cb.Resp  // 返回给客户端
```

**关键设计**:
- `Callback` 包含 `done chan struct{}`，用于同步等待
- `proposals` 列表通过 `index+term` 唯一标识每个请求
- Apply 时根据 `entry.Index` 找到对应的 `proposal` 并回调

### Key Design Patterns

- **Column Families**: Simulated via key prefixing (`${cf}_${key}`) in Badger
- **Ready Pattern**: Raft state changes returned via `Ready` struct, applied by upper layer
- **Region/Peer/Store**: Region = Raft group, Peer = Raft node, Store = Server instance
- **Two-Phase Commit**: Transactional API uses Percolator-style 2PC with primary/secondary keys
- **Follower Also Applies**: Unlike standard Raft (Leader-only apply), TinyKV has all nodes Apply committed entries to state machine. Benefits: (1) Fast failover - Follower can become Leader instantly, (2) Local reads - Follower can serve read-only requests, (3) State consistency - All nodes have identical KV data. This is a production-grade design choice inherited from TiKV.

### Resume-Worthy Highlights

- **Follower Apply 设计**：突破标准 Raft 的 Leader-only Apply 模式，实现所有节点同步 Apply 到状态机，支持快速故障切换和本地读取优化

- **CompactLog 防御性设计**：发现并修复 CompactLog 并发场景下的 LastCompactedIdx 竞争问题，通过 `CompactIndex > TruncatedState.Index` 双重检查防止日志回退截断，确保 GC 任务的范围正确性

- **Region Split 实现**：实现基于 SplitKey 的 Region 分裂机制，包括 RegionEpoch 版本控制、storeMeta 原子更新、新 Peer 创建与注册，以及 Split 后的心跳上报与负载均衡联动

- **Region 路由与 Key 查找**：深入理解分布式 KV 的 Region 划分机制，基于字节字典序（Lexicographical Order）的 Key 范围匹配算法，以及 B-Tree 驱动的高效 Region 路由表实现

### Core Design Decisions

- **Region Split 的核心动机**：解决分布式 KV 的数据/负载热点问题。单 Region 过大时会导致所有请求集中到一个 Leader，形成单点瓶颈。Split 将大 Region 拆分成多个小 Region，使 Scheduler 能够将不同 Region 的 Leader 分散到不同 Store，实现真正的水平扩展和负载均衡。这是 Auto-Sharding 的基石。

- **Split 只分裂数据，不改变副本配置**：Split 的本质是数据范围的水平分裂，不是副本配置的变更。如果原 Region 是 3 副本，分裂后的两个新 Region 都必须是 3 副本。通过检查 `len(oldRegion.Peers) == len(newPeerIds)` 确保配置一致性，拒绝过时的 Split 请求。

- **Split 新 Peer 的 StoreId 继承策略**：新 Region 的 Peer 继承旧 Region Peer 的 StoreId，而不是随机分配到新 Store。原因是 Split 前数据已在所有 Store 上完整复制，原地分裂无需数据搬迁，Split 操作瞬间完成。负载均衡由 Scheduler 后续通过 Region 搬迁实现（两阶段策略）。

- **Split 后新 Peer 的注册与启动流程**：Split 创建新 Region 的 Peer 后，通过 `router.register(peer)` 注册到路由表，再发送 `MsgTypeStart` 消息触发 `startTicker()` 启动 Raft 和 Heartbeat 定时器。采用消息驱动模型而非直接调用，保证并发安全和架构统一性。

### Problem-Solution Patterns

- **并发场景下的 Region 元数据深拷贝**：在发送 Region 心跳给 Scheduler 时，使用 `proto.Marshal + proto.Unmarshal` 实现 protobuf 消息的深拷贝（`CloneMsg` 函数），避免并发修改导致脏数据。问题背景：Peer 在发送心跳的同时可能被 Split 操作修改 Region 元数据，直接引用会导致 Scheduler 收到不一致的状态。解决方案：通过序列化 - 反序列化创建独立副本，保证数据传输的原子性。

- **深拷贝 vs 锁的权衡**：为什么选择深拷贝而不是锁？(1) 锁只能保护读取瞬间，不能保护异步发送过程（channel 传输中数据可能被修改）；(2) 锁持有期间阻塞可能导致死锁；(3) 网络传输本就需要序列化，深拷贝是"用确定的小开销换无死锁风险"。核心设计思想：跨线程/协程传输的数据必须独立拥有所有权（ownership transfer）。

### Important Protocols

- **Raft Messages**: Defined in `eraftpb.proto` (MsgAppend, MsgHeartbeat, MsgRequestVote, etc.)
- **KV Commands**: `kvrpcpb.proto` (RawGet/Put/Delete/Scan, KvGet/Prewrite/Commit/Scan)
- **Raft Commands**: `raft_cmdpb.proto` (Get/Put/Delete/Snap + Admin commands)
- **Admin Commands**: CompactLog, TransferLeader, ChangePeer, Split

## Development Notes

- Set `LOG_LEVEL=debug` for debugging
- Tests use mock network with configurable partitions and reliability
- Badger fork: `github.com/Connor1996/badger` (not the original dgraph-io/badger)
- Always call `Discard()` on badger.Txn and close iterators
