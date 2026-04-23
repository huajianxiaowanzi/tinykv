# Project 2C - 日志压缩与快照 个人笔记

## 一、2C 要解决什么问题？

Raft 日志无限增长 → 内存和磁盘爆满。需要两个机制：
1. **日志压缩（CompactLog）**：删掉已经 Apply 的旧日志
2. **快照（Snapshot）**：日志删了之后，落后节点无法通过日志追上 → 用快照把"执行结果"整个发过去

---

## 二、日志压缩 完整数据链路

```
┌──────────────────────────────────────────────┐
│ ① onRaftGcLogTick() — 定时器触发             │
│   位置: kv/raftstore/peer_msg_handler.go     │
│   触发: HandleMsg() → MsgTypeTick → onTick() │
│   只有 Leader 能发起                          │
│   条件: appliedIdx - firstIdx >= RaftLogGcCountLimit │
│   compactIdx = appliedIdx - 1                │
│   创建 CompactLogRequest(compactIdx, term)    │
│   调用 proposeRaftCommand(request, nil)       │
└──────────────────┬───────────────────────────┘
                   │ 和普通写入一样走 Raft 共识
                   ▼
┌──────────────────────────────────────────────┐
│ ② Raft 共识                                  │
│   CompactLogRequest 被封装成 entry            │
│   Leader 复制 → 多数确认 → Commit             │
└──────────────────┬───────────────────────────┘
                   │ Apply（所有节点，不仅仅是 Leader）
                   ▼
┌──────────────────────────────────────────────┐
│ ③ processAdminRequest() — Apply 时处理       │
│   位置: kv/raftstore/peer_msg_handler.go     │
│   防御性检查: compactIndex > truncatedState   │
│   更新 TruncatedState (index, term)           │
│   持久化 ApplyState 到 raftdb                 │
│   调用 ScheduleCompactLog(compactIndex)       │
└──────────────────┬───────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────┐
│ ④ ScheduleCompactLog() — 构造 GC 任务        │
│   RaftLogGCTask {StartIdx, EndIdx}            │
│   StartIdx = d.LastCompactedIdx               │
│   EndIdx = truncatedIndex + 1                 │
│   d.LastCompactedIdx = EndIdx                 │
│   发送到 raftLogGCTaskSender channel          │
└──────────────────┬───────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────┐
│ ⑤ RaftLogGCTaskHandler.Handle() — 物理删除   │
│   位置: kv/raftstore/runner/raftlog_gc.go     │
│   遍历 [StartIdx, EndIdx)                     │
│   逐条从 raftdb 中删除日志条目                │
└──────────────────────────────────────────────┘
```

### 关键设计：两阶段分离

| 阶段 | 操作 | 原因 |
|------|------|------|
| 元数据变更（更新 TruncatedState） | 走 Raft 共识 | 保证所有节点一致 |
| 物理删除（从 raftdb 删除旧日志） | 异步执行 | 不阻塞主流程 |

物理删除不需要共识——只要 TruncatedState 一致，各节点各自慢慢删，删多删少无所谓，下次 GC 会补上。

### compactIdx 为什么要减 1？

```go
compactIdx = appliedIdx - 1
```

必须保留一条已应用的日志作为截断边界。压缩到 `compactIdx` 意味着 `compactIdx` 之前的日志可以删，`compactIdx` 这条本身要保留。

### 所有节点都会删日志

- **Propose** 是 Leader 独占的权力
- **Apply** 是所有节点的义务（所有节点都会删自己的 raftdb 中的日志）
- CompactLog 走 Raft 保证了顺序性：Apply CompactLog 时，所有节点必然已 Apply 了 compactIdx 之前的全部日志

---

## 三、快照生成 完整数据链路

### 触发条件

Leader 在 `sendAppend()` 中发现 Follower 需要的日志已被截断：

```go
func (r *Raft) sendAppend(to uint64) bool {
    prevLogIndex := r.Prs[to].Next - 1
    prevLogTerm, err := r.RaftLog.Term(prevLogIndex)
    if err == nil {
        // 正常发送日志
        ...
        return true
    }
    // 获取任期出错 → prevLogIndex 在快照中 → 需要发快照
    r.sendSnapshot(to)
    return false
}
```

### sendSnapshot 逻辑

```go
func (r *Raft) sendSnapshot(to uint64) {
    snapshot, err := r.RaftLog.storage.Snapshot()
    if err != nil {
        // 快照还没生成好，等下次再试
        return
    }
    // 构造 MsgSnapshot 消息
    r.msgs = append(r.msgs, pb.Message{
        MsgType:  pb.MessageType_MsgSnapshot,
        From:     r.id,
        To:       to,
        Term:     r.Term,
        Snapshot: &snapshot,
    })
    // 乐观更新 Next（假设快照会成功送达）
    r.Prs[to].Next = snapshot.Metadata.Index + 1
}
```

### PeerStorage.Snapshot() — 异步生成快照

快照生成是耗时操作（需要扫描整个 Region 的 KV 数据），采用异步模式：

```
第 1 次调用 Snapshot():
  snapState = Relax → 发 RegionTaskGen 任务给 Region Worker
  → 返回 ErrSnapshotTemporarilyUnavailable

第 2 次调用 Snapshot():
  snapState = Generating → 非阻塞检查 channel
  → 还没生成完 → 返回 ErrSnapshotTemporarilyUnavailable

第 3 次调用 Snapshot():
  snapState = Generating → 非阻塞检查 channel
  → 有数据了 → 验证 → 返回快照 ✓
```

### Region Worker 生成快照的流程

```
handleGen(regionId, notifier)
    ↓
doSnapshot(engines, mgr, regionId)
    1. 从 raftdb 读取 RaftApplyState → 获取 appliedIndex 和 term
    2. 构造 SnapKey{RegionID, Term, Index}
    3. 从 kvdb 读取 RegionLocalState → 获取 Region 元信息
    4. 检查 Region 状态是否为 Normal
    5. 扫描 kvdb 中 [StartKey, EndKey) 的数据
    6. 按列族写入 SST 文件 + 生成 meta 文件
    7. 构造 eraftpb.Snapshot {Metadata, Data}
    8. 通过 notifier channel 发送结果
```

### 快照格式

**外层**：eraftpb.Snapshot（Raft 协议层）
```protobuf
Snapshot {
    Metadata {
        Index:     50,    // 快照包含的最后一条已应用日志的 index
        Term:      3,     // 对应的 term
        ConfState: ...,   // 集群配置（哪些节点在集群中）
    }
    Data: []byte          // 序列化后的 RaftSnapshotData
}
```

**内层**：rspb.RaftSnapshotData（TinyKV 应用层）
```protobuf
RaftSnapshotData {
    Region:   {...},      // Region 元信息（范围、Peers、Epoch）
    FileSize: 1024,       // 快照文件总大小
    Meta:     {...},      // 每个 CF 文件的校验和、大小
}
```

实际数据存在磁盘 SST 文件中：`gen_{RegionID}_{Term}_{Index}_{CF}.sst`

---

## 四、快照接收（Raft 层）

### handleSnapshot 逻辑

Follower 收到 `MsgSnapshot` 后，在 Raft 层的处理：

```go
func (r *Raft) handleSnapshot(m pb.Message) {
    meta := m.Snapshot.Metadata

    // 1. 拒绝过期快照：消息 term < 自身 term
    if m.Term < r.Term {
        resp.Reject = true
    // 2. 拒绝旧快照：已提交日志 >= 快照 index
    } else if r.RaftLog.committed >= meta.Index {
        resp.Reject = true
    // 3. 接受快照
    } else {
        r.becomeFollower(m.Term, m.From)  // 设置 Lead = m.From
        // 更新 RaftLog 指针
        r.RaftLog.dummyIndex = meta.Index + 1
        r.RaftLog.committed = meta.Index
        r.RaftLog.applied = meta.Index
        r.RaftLog.stabled = meta.Index
        r.RaftLog.pendingSnapshot = m.Snapshot  // 待应用快照
        r.RaftLog.entries = make([]pb.Entry, 0)
        // 根据 ConfState 更新集群配置
        r.Prs = make(map[uint64]*Progress)
        for _, id := range meta.ConfState.Nodes {
            r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1}
        }
    }
}
```

### pendingSnapshot 的作用

`pendingSnapshot` 是一个"缓冲区"——Raft 层接收快照后不直接应用，而是放在 `pendingSnapshot` 中，等下一次 `Ready()` 时交给上层（peer 层）处理。

```go
// rawnode.go Ready()
if !IsEmptySnap(rn.Raft.RaftLog.pendingSnapshot) {
    rd.Snapshot = *rn.Raft.RaftLog.pendingSnapshot
}
```

---

## 五、快照应用（Peer 层）

### 完整数据链路

```
HandleRaftReady()
    ↓
rd := d.RaftGroup.Ready()  // Ready 中包含 Snapshot
    ↓
SaveReadyState(rd)          // 持久化 + 应用快照
    ↓
ApplySnapshot(snapshot, kvWB, raftWB)
    ↓
1. 删除旧数据
   - clearMeta(kvWB, raftWB)     → 删除 kvdb/raftdb 中的旧元数据
   - clearExtraData(snapData.Region) → 删除 kvdb 中旧 Region 范围的 KV 数据
    ↓
2. 更新内存状态
   - raftState.LastIndex = snapshot.Metadata.Index
   - raftState.LastTerm = snapshot.Metadata.Term
   - applyState.AppliedIndex = snapshot.Metadata.Index
   - applyState.TruncatedState = {Index, Term} = snapshot.Metadata
   - snapState = SnapState_Applying
    ↓
3. 持久化 ApplyState 到 raftdb
    ↓
4. Region Worker 写入 KV 数据
   - 发送 RegionTaskApply 给 region worker
   - 同步等待完成 (<-ch)
    ↓
5. 更新 RegionLocalState → 写入 kvdb
    ↓
返回 ApplySnapResult {PrevRegion, Region}
```

### 为什么三个值都设成快照的 Index？

```
AppliedIndex = 50    → 状态机已应用到第 50 条
TruncatedState = 50  → 第 1~50 条日志已被截断
LastIndex = 50       → 日志从第 50 条开始（之前的都没了）
```

快照的含义："第 1~50 条日志的执行结果，已经全部体现在快照中了"。

### 快照数据存哪里？

| 数据 | 存储位置 | 原因 |
|------|---------|------|
| KV 键值对（状态机数据） | kvdb | 业务数据，客户端读写的是它 |
| RaftApplyState + TruncatedState | raftdb | Raft 协议状态，崩溃恢复用 |
| RaftLocalState (LastIndex 等) | raftdb | Raft 协议状态 |
| RegionLocalState | kvdb | Region 元信息 |

---

## 六、遇到的 Bug 和踩坑

### Bug 1: handleSnapshot 中注释掉了 ConfState 更新

**现象**：`TestProvideSnap2C` 空指针崩溃
```
panic: runtime error: invalid memory address or nil pointer dereference
at raft_test.go:1066  sm.Prs[2].Next = 10
```

**原因**：`handleSnapshot` 中更新集群配置的代码被注释掉了：
```go
// r.Prs = make(map[uint64]*Progress)
// for _, id := range meta.ConfState.Nodes {
//     r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1}
// }
```

测试流程：
1. `newTestRaft(1, [1], ...)` → Prs 只有 {1}
2. `handleSnapshot(ConfState={1,2})` → 应该把节点 2 也加到 Prs 中
3. `becomeLeader()` → 成为 Leader
4. `sm.Prs[2].Next = 10` → 访问 Prs[2] → nil → 崩溃

**修复**：取消注释。快照的 `ConfState` 代表集群节点配置，必须更新 `Prs`。

### Bug 2: TestRestoreFromSnapMsg2C 失败

**现象**：`sm.Lead = 0, want 1`

**原因**：和 Bug 1 同根——ConfState 没更新导致后续逻辑异常。取消注释后自动修复。

---

## 七、核心设计总结

| 机制 | 核心思想 |
|------|---------|
| 日志压缩 | 元数据变更走 Raft 共识，物理删除异步执行 |
| 快照生成 | 耗时操作交给 Region Worker 异步做，第一次"下单"，下次"取货" |
| 快照发送 | 乐观更新 Next + 心跳机制自愈（丢了不慌，下次重发） |
| 快照接收 | Raft 层放 pendingSnapshot，Ready 交给 peer 层应用 |
| 快照应用 | 全量替换（先清旧数据，再写新数据），同步等待 Region Worker 完成 |

### 一句话总结每个流程

- **CompactLog**：定时检查日志堆积量，超过阈值就通过 Raft 提议压缩，各节点 Apply 后各自异步删除旧日志
- **Snapshot 生成**：Leader 发现 Follower 需要的日志已被压缩，触发 Region Worker 异步生成快照
- **Snapshot 接收**：Follower 在 Raft 层更新指针和集群配置，把快照放入 pendingSnapshot 等待上层处理
- **Snapshot 应用**：清空旧数据 → 用快照重置所有状态 → Region Worker 写入 KV 数据 → 持久化元数据
