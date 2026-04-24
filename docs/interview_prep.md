# TinyKV 面试复习手册

## 使用说明

每个项目按三层金字塔复习：
1. **是什么**：功能描述，解决的问题
2. **怎么做**：数据流图，关键代码路径
3. **为什么**：设计决策，对比分析，取舍原因

---

# Project 1: Standalone KV Storage

## 一、是什么

**功能**：单机 KV 存储引擎，基于 Badger 实现。

**解决的问题**：
- 提供基础的 Key-Value 存储能力
- 模拟 Column Family（CF）机制
- 为后续 Raft 层、事务层提供存储接口

## 二、怎么做

### 数据结构

```
Badger (LSM Tree 存储)
    │
    ├─ Default CF  → 用户数据
    ├─ Lock CF     → 事务锁
    └─ Write CF    → 版本记录
```

**CF 模拟方式**：
```go
// Badger fork 不支持原生 CF
// 用 key prefix 模拟：
key = "cf_prefix + user_key"

// 例如：
Default CF: "d" + "user_key"
Lock CF:    "l" + "user_key"
Write CF:   "w" + "user_key"
```

### 读写接口

```go
// 读接口
type Reader interface {
    GetCF(cf string, key []byte) []byte
    IterCF(cf string) Iterator
}

// 写接口
type Storage interface {
    Write(ctx, writes []Write) error
}

type Write struct {
    Cf    string   // 目标 CF
    Key   []byte   // key
    Value []byte   // value (nil = delete)
}
```

### 数据流图

```
写入请求：
    Client.Put("foo", "bar")
        ↓
    StandaloneStorage.Write()
        ↓
    Badger.Txn.Set("d_foo", "bar")
        ↓
    写入 LSM Tree

读取请求：
    Client.Get("foo")
        ↓
    StandaloneStorage.Reader()
        ↓
    Reader.GetCF(CfDefault, "foo")
        ↓
    Badger.Get("d_foo")
        ↓
    返回 "bar"
```

## 三、为什么

### 为什么用 Badger 而不是 RocksDB？

| 特性 | Badger | RocksDB |
|------|---------|----------|
| 语言 | Go | C++ |
| LSM Tree | 是 | 是 |
| 原生 CF | 不支持（需 fork） | 支持 |
| 适用场景 | Go 项目 | 生产级高性能 |

**TinyKV 选择 Badger**：
- 项目是 Go 实现
- Badger fork 版本支持 CF 模拟
- 学习项目，不需要极致性能

### 为什么用 key prefix 模拟 CF？

```
原生 CF（RocksDB）：
  不同 CF 物理隔离，独立 LSM Tree
  
模拟 CF（Badger fork）：
  所有数据在同一 LSM Tree，用 prefix 区分
  
优势：
  1. 实现简单，不需要改 Badger 核心
  2. 事务可以跨 CF（用 Badger 原生事务）
  
劣势：
  1. 无物理隔离，压缩时影响所有 CF
  2. 无法针对不同 CF 调优
```

## 四、面试要点

**可能的问题**：

Q: "Badger 怎么存储数据？"
A: LSM Tree 结构，数据先写内存表（MemTable），定期刷到磁盘 SST 文件。读取时先查 MemTable，再查 SST。

Q: "为什么用 Go 实现？"
A: 学习项目，Go 更容易理解。TiKV 用 Rust，性能更高。

Q: "CF 模拟的优缺点？"
A: 优点是实现简单、跨 CF 事务方便。缺点是无法物理隔离、调优受限。

---

# Project 2A: Raft 实现

## 一、是什么

**功能**：实现 Raft 共识算法核心状态机。

**解决的问题**：
- 保证分布式系统数据一致性
- Leader 选举 + 日志复制 + 心跳维护

## 二、怎么做

### 核心状态

```go
type Raft struct {
    // 身份
    State       StateType    // Leader/Follower/Candidate
    
    // 时间
    CurrentTerm uint64       // 当前任期
    Lead        uint64       // Leader ID
    
    // 日志
    RaftLog     *RaftLog     // 日志存储
    
    // 选举
   Votes       map[uint64]bool   // 收到的投票
    ElectionTimeout int         // 选举超时（随机）
    
    // 复制
    Prs        map[uint64]*Progress  // Follower 复制进度
    PendingCommitted int           // 待提交数量
    
    // 消息
    msgs       []Message          // 待发送消息
    Step       func(m Message)    // 状态处理函数
}
```

### 选举流程

```
┌─────────────────────────────────────────────────────────┐
│ Follower 状态                                            │
│ - 收到 MsgHeartbeat → 重置 electionElapsed              │
│ - 选举超时 → 变 Candidate                                │
└─────────────────────────────────────────────────────────┘
                    │ electionElapsed >= ElectionTimeout
                    ↓
┌─────────────────────────────────────────────────────────┐
│ Candidate 状态                                           │
│ - CurrentTerm++                                          │
│ - Votes[self] = true                                     │
│ - 发送 MsgRequestVote 给所有 Peer                         │
└─────────────────────────────────────────────────────────┘
                    │
                    ↓
┌─────────────────────────────────────────────────────────┐
│ 收到投票                                                 │
│ - MsgRequestVoteResponse.Reject == false → Votes[id]=true│
│ - len(Votes) > len(Prs)/2 → 变 Leader                    │
└─────────────────────────────────────────────────────────┘
                    │
        ┌───────────┼───────────┐
        │ 多数票    │ 少数票     │ 收到更高 term
        ↓           ↓           ↓
┌───────────┐ ┌───────────┐ ┌───────────┐
│ Leader    │ │ Follower  │ │ Follower  │
│ 发心跳    │ │ 等待下次  │ │ 更新 term │
└───────────┘ └───────────┘ └───────────┘
```

### 日志复制流程

```
┌─────────────────────────────────────────────────────────┐
│ Leader 收到 Propose                                       │
│ - append log: RaftLog.Append(Entry{Term, Index, Data})   │
│ - PendingCommitted++                                      │
│ - 更新 Prs[self].Match = lastIndex                        │
└─────────────────────────────────────────────────────────┘
                    │
                    ↓
┌─────────────────────────────────────────────────────────┐
│ 发送 MsgAppend                                            │
│ - 遍历所有 Follower                                       │
│ - MsgAppend.PrevLogIndex = Prs[id].Next - 1              │
│ - MsgAppend.PrevLogTerm = RaftLog[PrevLogIndex].Term     │
│ - MsgAppend.Entries = RaftLog[Next:]                     │
│ - MsgAppend.CommitIndex = RaftLog.Committed              │
└─────────────────────────────────────────────────────────┘
                    │
                    ↓
┌─────────────────────────────────────────────────────────┐
│ Follower 收到 MsgAppend                                   │
│ - 检查 PrevLogIndex/PrevLogTerm 是否匹配                  │
│   ├─ 匹配 → append entries, 返回 MsgAppendResponse(Reject=false)│
│   └─ 不匹配 → 拒绝, 返回 MsgAppendResponse(Reject=true)   │
└─────────────────────────────────────────────────────────┘
                    │
                    ↓
┌─────────────────────────────────────────────────────────┐
│ Leader 收到 MsgAppendResponse                             │
│ - Reject == false → Prs[id].Match = lastIndex, Next++    │
│ - Reject == true → Next-- (回退)                          │
│ - 检查是否多数已复制:                                      │
│   if matchCount > len(Prs)/2:                             │
│       Committed = max(Committed, lastIndex)               │
│       PendingCommitted--                                   │
└─────────────────────────────────────────────────────────┘
```

### 心跳流程

```
Leader 定时器触发 (heartbeatElapsed >= heartbeatTimeout)
    │
    ↓
发送 MsgHeartbeat 给所有 Follower
    │
    ↓
Follower 收到 MsgHeartbeat
    ├─ term < CurrentTerm → 拒绝 (MsgHeartbeatResponse.Reject=true)
    ├─ term > CurrentTerm → 更新 term, 变 Follower
    └─ term == CurrentTerm → 重置 electionElapsed, 返回确认
    │
    ↓
Leader 收到 MsgHeartbeatResponse
    ├─ Reject=true → term 更新, 变 Follower
    └─ Reject=false → Lead 确认
```

### 状态转换图

```
         ┌─────────────────────────────────────┐
         │                                     │
         │     ┌───────────────────┐           │
         │     │                   │           │
    超时  │     │    Follower       │←─────────┼─── term 更新
    无Leader│     │                   │           │
         │     └───────────────────┘           │
         │              │                       │
         │              │ 选举超时              │
         │              ↓                       │
         │     ┌───────────────────┐           │
         │     │                   │           │
         │     │    Candidate      │           │
         │     │                   │           │
         │     └───────────────────┘           │
         │              │                       │
         │              │ 多数票                │
         │              ↓                       │
         │     ┌───────────────────┐           │
         └───→│                   │←──────────┼─── 多数确认
               │    Leader         │           │
               │                   │──────────┼─── 发送 heartbeat
               └───────────────────┘           │
```

## 三、为什么

### 为什么用随机超时避免分裂票？

```
固定超时：
  所有 Follower 同时超时 → 同时 Candidate → 票数分散 → 无 Leader
  
随机超时：
  Follower A: 超时 150ms
  Follower B: 超时 200ms
  Follower C: 超时 180ms
  
  → A 先超时 → 成为 Candidate → 发送投票请求
  → B 和 C 还在等待 → 收到 A 的请求 → 投票给 A
  → A 获得多数票 → 成为 Leader
```

### 为什么 Follower 要拒绝 term 更低的请求？

```
场景：
  旧 Leader (term=3) 网络隔离后恢复
  发送 MsgHeartbeat 给新 Leader (term=5)
  
处理：
  新 Leader 收到 term=3 的消息 → 拒绝
 旧 Leader 收到拒绝 → 发现 term=5 → 更新 term → 变 Follower
  
原因：
  term 代表"新鲜度"，term 高的节点拥有更新的信息
  接受 term 低的消息会导致状态回退，违反一致性
```

### 为什么用 MatchIndex + NextIndex？

```
MatchIndex: 已确认复制的最高日志 index
NextIndex: 下一条要发送的日志 index

Leader 初始化：
  NextIndex = Leader.LastIndex + 1  (乐观假设 Follower 已同步)
  
收到拒绝：
  NextIndex-- (回退，逐条重试)
  
收到确认：
  MatchIndex = lastIndex
  NextIndex = lastIndex + 1

设计思想：
  乐观初始化 + 保守回退
  避免 Leader 维护所有 Follower 的完整日志副本
```

## 四、面试要点

**必问问题**：

Q: "Raft 怎么选举 Leader？"
A: Follower 超时变 Candidate，增加 term，发送 MsgRequestVote。收到多数票后变 Leader。随机超时避免分裂票。

Q: "日志怎么复制？"
A: Leader 收到 propose → append log → 发送 MsgAppend → Follower 检查 PrevLogIndex/PrevLogTerm → 匹配则 append。Leader 收到多数确认后 commit。

Q: "Follower 怎么知道 Leader 存活？"
A: Leader 定时发送 MsgHeartbeat。Follower 收到后重置选举超时。

Q: "收到更高 term 的消息怎么办？"
A: 无论当前状态，都更新 term 并变 Follower。这是 Raft 的"认主"规则。

Q: "和 Raft 论文有什么区别？"
A: 论文是框架描述，我实现了完整状态机。一个设计区别：Follower 也 Apply（Project 2B）。

---

# Project 2B: Raft KV

## 一、是什么

**功能**：基于 Raft 实现分布式 KV 存储。

**解决的问题**：
- 写请求通过 Raft 共识，保证一致性
- 读请求可以从 Leader 或 Follower 获取

## 二、怎么做

### 整体架构

```
┌────────────────────────────────────────────────────────────┐
│                        Client                               │
│                     (RPC Request)                           │
└────────────────────────────────────────────────────────────┘
                         │
                         ↓
┌────────────────────────────────────────────────────────────┐
│                    RaftStorage                              │
│                  (kv/storage/raft_storage)                  │
│                                                             │
│  Write() → SendRaftCommand(request, callback)              │
│  Read()  → 直接读 kvdb（Leader 只有）                        │
└────────────────────────────────────────────────────────────┘
                         │
                         ↓
┌────────────────────────────────────────────────────────────┐
│                   RaftstoreRouter                           │
│                  (消息路由层)                                │
│                                                             │
│  SendRaftCommand → MsgRaftCmd → raftCh                      │
└────────────────────────────────────────────────────────────┘
                         │
                         ↓
┌────────────────────────────────────────────────────────────┐
│                     raftWorker                              │
│                  (Raft 消息处理循环)                          │
│                                                             │
│  for msg := range raftCh {                                  │
│      HandleMsg(msg)                                         │
│  }                                                          │
└────────────────────────────────────────────────────────────┘
                         │
         ┌───────────────┼───────────────┐
         ↓               ↓               ↓
┌─────────────┐   ┌─────────────┐   ┌─────────────┐
│ PeerMsgHandler│   │  Raft Tick   │   │  Apply Task  │
│ (消息处理)    │   │  (定时器)     │   │  (应用日志)   │
└─────────────┘   └─────────────┘   └─────────────┘
```

### 写请求完整数据流

```
① 客户端请求
    Put("foo", "bar")
        ↓
② RaftStorage.Write()
    cb := message.NewCallback()
    request := &RaftCmdRequest{Requests: [Put{Key:"foo", Value:"bar"}]}
    rs.raftRouter.SendRaftCommand(request, cb)
        ↓
③ RaftstoreRouter.SendRaftCommand()
    msg := &MsgRaftCmd{Request: request, Callback: cb}
    router.peerSender <- msg
        ↓
④ raftWorker.run()
    msg := range rw.raftCh
    peerMsgHandler.HandleMsg(msg)
        ↓
⑤ HandleMsg() → proposeRaftCommand()
    d.proposals = append(&proposal{
        index: d.RaftGroup.Raft.LastIndex() + 1,
        term:  d.RaftGroup.Raft.Term,
        cb:    cb,
    })
    d.RaftGroup.Propose(data)
        ↓
⑥ Raft 内部处理
    - 创建 Entry{Term, Index, Data}
    - 发送 MsgAppend 给 Follower
    - 等待多数确认
    - 标记 Committed
        ↓
⑦ HandleRaftReady()
    rd := d.RaftGroup.Ready()
    
    // 持久化
    d.peerStorage.SaveReadyState(rd)→ raftdb
    
    // 发送消息
    for msg := range rd.Messages {
        send(msg)
    }
    
    // Apply 日志
    applyCommittedEntries(rd.CommittedEntries)
        ↓
⑧ applyCommittedEntries()
    for entry := range entries {
        cmd := Unmarshal(entry.Data)
        execWriteRequest(cmd)  // 写入 kvdb
        onRaftBaseResp(cmd, entry.Index)
    }
        ↓
⑨ onRaftBaseResp()
    for p := range d.proposals {
        if p.index == entry.Index && p.term == entry.Term {
            p.cb.Done(resp)  // 触发回调！
            删除 proposal
            break
        }
    }
        ↓
⑩ Callback.Done()
    cb.done <- struct{}{}
        ↓
⑪ RaftStorage.Write() 返回
    cb.WaitResp()  // 等待 done channel
    return cb.Resp
        ↓
⑫ 返回客户端
```

### proposal 结构详解

```go
type proposal struct {
    index uint64        // 日志在 Raft 中的位置
    term  uint64        // 任期号
    cb    *Callback     // 客户端回调
}

type Callback struct {
    Resp interface{}
    done chan struct{}  // 用于同步等待
}
```

**作用**：
- `index + term` 唯一标识一个请求
- Apply 时根据 entry.Index 找到对应的 proposal
- 通过 cb.Done() 触发回调，通知客户端

**为什么需要 proposal 列表？**

```
场景：多个并发请求

Request 1: index=5, term=3
Request 2: index=6, term=3
Request 3: index=7, term=3

Raft 只返回 entry，不知道哪个 entry 对应哪个请求

proposal 列表：
  [{index:5, cb:cb1}, {index:6, cb:cb2}, {index:7, cb:cb3}]

Apply 时：
  entry{Index:6} → 匹配 proposal[1] → cb2.Done()
```

### Follower Apply 流程

```
Follower 收到 MsgAppend
    │
    ↓
检查 PrevLogIndex/PrevLogTerm
    │
    ↓
append entries → RaftLog.Logs
    │
    ↓
更新 Committed = min(LeaderCommit, lastIndex)
    │
    ↓
HandleRaftReady()
    │
    ↓
applyCommittedEntries()  ← Follower 也执行！
    │
    ↓
写入 kvdb（和 Leader 数据一致）
```

## 三、为什么

### 为什么 Follower 也 Apply？（面试核心）

**标准 Raft**：只有 Leader Apply，Follower 只存储日志。

**TinyKV/TiKV 设计**：所有节点都 Apply。

**好处**：

```
1. 快速故障切换
   Leader 挂掉 → Follower 立即成为 Leader
   不需要先同步 Apply 未完成的日志
   
2. 本地读取
   Follower 可以直接读 kvdb
   不需要向 Leader 请求
   
3. 状态一致性
   所有节点 kvdb 数据完全一致
   便于调试和维护
```

**代价**：
- Follower Apply 延迟（Leader 先 Apply）
- 需要更多磁盘写入

**对比**：
| 设计 | Leader Apply | Follower 也 Apply |
|------|--------------|-------------------|
| 故障切换 | 需先 Apply | 立即可用 |
| 读性能 | 只能读 Leader | 可读任意节点 |
| 写入量 | 少 | 多 |

### 为什么用 Callback 而不是阻塞等待？

```
阻塞等待：
  RaftStorage.Write() → 阻塞等待 Raft 完成 → 返回
  
问题：
  阻塞期间占用线程
  无法处理其他请求

Callback 模式：
  RaftStorage.Write() → 发送请求 → 返回 goroutine
  
  goroutine 内：
    cb.WaitResp() → 阻塞等待回调
  
好处：
  不阻塞主线程
  每个请求独立 goroutine
```

### 为什么 SaveReadyState 要先于 Apply？

```
顺序：
  1. SaveReadyState(rd)  → 持久化 Raft 状态
  2. Apply entries       → 写入 kvdb

原因：
  如果先 Apply 后持久化：
    Apply 成功 → 持久化失败 → 重启后日志重放 → 重复 Apply
    
  先持久化后 Apply：
    持久化成功 → Apply 成功 → 正常
    持久化成功 → Apply 失败 → 重启后日志重放 → 重新 Apply（幂等）
```

## 四、面试要点

**核心问题**：

Q: "写请求的完整流程？"
A: 客户端 → RaftStorage → propose → Raft 复制 → Ready → Apply → callback → 客户端。关键是 proposal 机制，通过 index+term 匹配请求和响应。

Q: "Follower 为什么也 Apply？"
A: TiKV 设计，三个好处：快速故障切换、本地读取、状态一致性。代价是写入量和延迟。

Q: "proposal 是什么？"
A: index+term+callback 的三元组。Raft 只返回 entry，proposal 列表用于匹配 entry 和客户端请求。

Q: "RaftStorage 怎么等结果？"
A: Callback 包含 done channel。Apply 后 cb.Done() 发送信号，RaftStorage.WaitResp() 收到信号后返回。

---

# Project 2C: Snapshot + Log GC

## 一、是什么

**功能**：
- Snapshot：新节点/落后节点通过快照同步数据
- Log GC：压缩 Raft 日志，防止无限增长

**解决的问题**：
- 新节点加入时需要同步全部数据
- Follower 日志落后太多无法通过 MsgAppend 同步
- 日志无限增长占用存储

## 二、怎么做

### Snapshot 流程

```
┌─────────────────────────────────────────────────────────┐
│ ① Follower 日志落后                                      │
│    Leader: lastIndex=100, commitIndex=100                │
│    Follower: lastIndex=10                                │
│    MsgAppend.PrevLogIndex=11 → Follower 没有              │
└─────────────────────────────────────────────────────────┘
                    │ Reject=true
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ② Leader 决定发 Snapshot                                  │
│    if Prs[follower].Next <= RaftLog.truncatedIndex       │
│       发送 MsgSnapshot                                    │
└─────────────────────────────────────────────────────────┘
                    │
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ③ 生成 Snapshot                                           │
│    snapshot := d.peerStorage.Snapshot()                  │
│    → 读取 kvdb 全部数据                                    │
│    → 生成 sst 文件                                         │
└─────────────────────────────────────────────────────────┘
                    │
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ④ 发送 MsgSnapshot                                        │
│    MsgSnapshot{                                          │
│        Metadata: {Index, Term, ConfState}                │
│        Data: snapshot bytes                              │
│    }                                                     │
└─────────────────────────────────────────────────────────┘
                    │
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ⑤ Follower 收到 Snapshot                                  │
│    - 应用 Snapshot 到 kvdb                                │
│    - 更新 RaftLog.truncatedIndex/term                     │
│    - 删除 truncatedIndex 之前的日志                        │
│    - 更新 appliedIndex                                    │
└─────────────────────────────────────────────────────────┘
```

### CompactLog 流程

```
┌─────────────────────────────────────────────────────────┐
│ ① onRaftGcLogTick() — 定时触发                            │
│    只有 Leader 能发起                                      │
│    检查: appliedIdx - firstIdx >= batch size              │
│    compactIdx = appliedIdx - 1                           │
│    创建 CompactLogRequest(compactIdx, term)              │
└─────────────────────────────────────────────────────────┘
                    │ proposeRaftCommand(request, nil)
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ② Raft 共识 — 和普通写入一样                               │
│    Leader 复制 → 多数确认 → Commit                         │
└─────────────────────────────────────────────────────────┘
                    │ Apply (所有节点)
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ③ processAdminRequest()                                  │
│    防御性检查: compactIndex > truncatedState              │
│    更新 TruncatedState(index=compactIdx, term)           │
│    调用 ScheduleCompactLog(compactIndex)                 │
└─────────────────────────────────────────────────────────┘
                    │ 发送任务到 channel
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ④ ScheduleCompactLog()                                   │
│    task := RaftLogGCTask{                                │
│        StartIdx: d.LastCompactedIdx                       │
│        EndIdx: compactIndex + 1                          │
│    }                                                     │
│    d.LastCompactedIdx = EndIdx                           │
│    发送 task                                             │
└─────────────────────────────────────────────────────────┘
                    │
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ⑤ RaftLogGCTaskHandler.Handle()                          │
│    遍历 [StartIdx, EndIdx)                               │
│    删除 raftdb 中的日志条目                                │
└─────────────────────────────────────────────────────────┘
```

## 三、为什么

### 为什么 CompactLog 要走 Raft 共识？

```
不走 Raft：
  Leader: truncIndex=50
  Follower A: truncIndex=40  ← 自己决定
  Follower B: truncIndex=60  ← 自己决定
  
  问题：
  Follower A 重启 → truncIndex=40 → 重放 log[41-50]
  但 log[41-50] 在 Leader 已 trunc → 数据不一致
  
走 Raft：
  Leader 发 CompactLog → 所有节点 Apply → truncIndex 一致=50
  
  好处：
  1. TruncatedState 一致
  2. Snapshot 同步时不会误删
  3. 重启后重放起点一致
```

### 为什么物理删除异步执行？

```
同步删除：
  Apply CompactLog → 删除 log[1-50] → 阻塞
  
问题：
  1. 删除耗时，阻塞 Apply
  2. 删除失败会影响事务
  
异步删除：
  Apply CompactLog → 更新 TruncatedState → 返回
  GC Task → 后台慢慢删除
  
好处：
  1. 不阻塞主流程
  2. 删除失败不影响一致性（下次 GC 补上）
  3. 各节点可以独立进度
```

### 为什么 compactIdx = appliedIdx - 1？

```
appliedIdx = 100

如果 compactIdx = 100：
  truncIndex=100 → log[1-100] 删除
  
  问题：
  重启后从 log[100] 开始 → 但 log[100] 已删除
  
正确做法 compactIdx = 99：
  truncIndex=99 → log[1-99] 删除
  log[100] 保留 → 重启后有边界
  
原因：
  必须保留一条已 Apply 的日志作为截断边界
```

## 四、面试要点

Q: "Snapshot 什么时候触发？"
A: Follower 日志落后，PrevLogIndex 超出 Leader 的 truncatedIndex。

Q: "CompactLog 为什么走 Raft？"
A: 保证所有节点 TruncatedState 一致。如果各节点独立 trunc，重启后重放起点不一致。

Q: "为什么物理删除异步？"
A: 不阻塞主流程。删除失败不影响一致性，下次 GC 补上。

Q: "为什么 compactIdx - 1？"
A: 保留一条已 Apply 日志作为截断边界，重启后能找到起点。

---

# Project 3A: Leader Transfer + Conf Change

## 一、是什么

**功能**：
- Leader Transfer：主动转移 Leader
- Conf Change：动态增减节点

**解决的问题**：
- 负载均衡需要转移 Leader
- 故障恢复需要移除节点
- 扩容需要增加节点

## 二、怎么做

### Leader Transfer 流程

```
┌─────────────────────────────────────────────────────────┐
│ ① 客户端请求 TransferLeader(target)                       │
└─────────────────────────────────────────────────────────┘
                    │
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ② Leader 检查目标节点日志                                  │
│    if Prs[target].Match < lastIndex:                     │
│       发送 MsgAppend → 同步日志                            │
│    等待 Match == lastIndex                               │
└─────────────────────────────────────────────────────────┘
                    │
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ③ 发送 MsgTimeoutNow                                      │
│    目标节点立即触发选举                                    │
└─────────────────────────────────────────────────────────┘
                    │
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ④ 目标节点收到 MsgTimeoutNow                              │
│    立即变 Candidate → 发送 MsgRequestVote                 │
│    自己投票 + 其他节点投票 → 变 Leader                     │
└─────────────────────────────────────────────────────────┘
```

### Conf Change 流程

```
┌─────────────────────────────────────────────────────────┐
│ ① 客户端请求 ChangePeer(AddNode/RemoveNode)               │
└─────────────────────────────────────────────────────────┘
                    │ proposeRaftCommand
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ② Raft 共识                                               │
│    ConfChange → Entry → 复制 → Commit                     │
└─────────────────────────────────────────────────────────┘
                    │ Apply
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ③ Apply ConfChange                                        │
│    - 更新 Raft.Prs (增加/删除节点)                         │
│    - 更新 Region.Peers                                    │
│    - 更新 Region.RegionEpoch.conf_ver++                  │
└─────────────────────────────────────────────────────────┘
```

## 三、为什么

### 为什么 Leader Transfer 先同步日志？

```
不同步：
  Leader: lastIndex=100
  Target: Match=50
  
  Transfer → Target 成为 Leader
  → 但 log[51-100] 缺失 → 数据丢失
  
同步后：
  Leader 发 MsgAppend → Target Match=100
  Transfer → Target 成为 Leader → 数据完整
```

### 为什么用 MsgTimeoutNow 而不是等超时？

```
等超时：
  Target 等随机超时（150-300ms）
  → 延迟高
  
MsgTimeoutNow：
  Target 立即选举
  → 快速完成转移
```

### 为什么 Conf Change 走 Raft？

```
不走 Raft：
  Leader 直接加节点
  → 不同节点配置不一致
  → 选举票数计算错误
  
走 Raft：
  ConfChange → 共识 → 所有节点配置一致
  → 选举、日志复制正确
```

## 四、面试要点

Q: "Leader Transfer 怎么保证数据完整？"
A: 先同步日志，确保目标节点 Match == lastIndex，再发送 MsgTimeoutNow。

Q: "Conf Change 为什么走 Raft？"
A: 配置变更需要共识，保证所有节点配置一致。

---

# Project 3B: Region Split

## 一、是什么

**功能**：将大 Region 分裂成多个小 Region。

**解决的问题**：
- 数据热点：单 Region 过大，请求集中
- 负载均衡：需要多 Region 才能分散 Leader

## 二、怎么做

### Split 流程

```
┌─────────────────────────────────────────────────────────┐
│ ① SplitCheck 定时检测                                     │
│    检查 Region 大小 > 阈值                                 │
│    找 SplitKey（按大小/Key 数）                            │
│    发送 Split Request                                     │
└─────────────────────────────────────────────────────────┘
                    │ proposeRaftCommand
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ② Raft 共识                                               │
│    AdminRequest(Split) → Entry → 复制 → Commit            │
└─────────────────────────────────────────────────────────┘
                    │ Apply (所有节点)
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ③ Apply Split                                             │
│    - 创建新 Region (newRegion)                            │
│    - newRegion.Peers = 继承 oldRegion.Peers 的 StoreId    │
│    - newRegion.RegionEpoch = {conf_ver: old, version: old+1}│
│    - oldRegion.RegionEpoch.version++                     │
│    - oldRegion.EndKey = SplitKey                         │
└─────────────────────────────────────────────────────────┘
                    │
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ④ 更新元数据                                               │
│    storeMeta.regions:                                     │
│      - 删除 oldRegion（整个范围）                          │
│      - 添加 oldRegion（新范围）                            │
│      - 添加 newRegion                                     │
│    storeMeta.regionRanges (B-Tree):                       │
│      - 更新路由表                                          │
└─────────────────────────────────────────────────────────┘
                    │
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ⑤ 创建新 Peer                                             │
│    newPeer := createPeer(newRegion)                      │
│    router.register(newPeer)                              │
│    发送 MsgTypeStart → 启动 Raft 定时器                   │
└─────────────────────────────────────────────────────────┘
                    │
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ⑥ 心跳上报                                                │
│    新 Region 向 Scheduler 发送心跳                         │
│    Scheduler 记录新 Region                                │
│    后续做负载均衡                                          │
└─────────────────────────────────────────────────────────┘
```

### RegionEpoch 版本号

```go
type RegionEpoch struct {
    ConfVer    uint64  // 配置变更版本
    Version    uint64  // 范围变更版本
}
```

```
ConfVer 追踪：Peer 增删
  ChangePeer → ConfVer++

Version 追踪：Key 范围变更
  Split → Version++
  Merge → Version++

作用：
  拒绝过时请求
  
  例：
  Region A: Version=1, Range=[a, z)
  Split → Region A: Version=2, Range=[a, m)
          Region B: Version=2, Range=[m, z)
  
  客户端用 Version=1 请求 Range=[n, o)
  → Region B 检查 Version=1 < 2
  → 拒绝，返回 RegionError
```

## 三、为什么

### 为什么用 Range 分片而非 Hash？

```
Hash 分片：
  key → hash(key) % N → 节点
  
  Scan("a", "z"):
    需要访问所有节点 → 性能差
  
  Split/扩容：
    N 变化 → 所有 key 重新分布 → 数据搬迁量大

Range 分片：
  Key 按字典序排列
  Region 1: [a, m)
  Region 2: [m, z)
  
  Scan("a", "z"):
    可能只访问连续几个 Region → 性能好
  
  Split:
    只改元数据，不搬数据 → 快
```

### 为什么新 Peer 继承 StoreId？

```
不继承（随机分配）：
  原 Region 3 副本在 Store [1, 2, 3]
  新 Region 分到 Store [4, 5, 6]
  
  → 需要把数据从 [1,2,3] 拷贝到 [4,5,6]
  → 搬迁量大，耗时

继承：
  原 Region 3 副本在 Store [1, 2, 3]
  新 Region 3 副本也在 Store [1, 2, 3]
  
  → 数据已在各 Store 上完整复制
  → 原地分裂，瞬间完成
  → 负载均衡由 Scheduler 后续 Region 搬迁实现
```

### 为什么 Split 走 Raft？

```
不走 Raft：
  不同节点可能在不同时机 Split
  → Region 信息不一致
  → 客户端请求路由错误
  
走 Raft：
  Split → 共识 → 所有节点同时 Split
  → Region 信息一致
```

## 四、面试要点

Q: "为什么用 Range 分片？"
A: Scan 友好（连续 Key 在同一 Region）；Split 只改元数据不搬数据。

Q: "新 Peer 为什么继承 StoreId？"
A: 数据已在各 Store 完整复制，原地分裂瞬间完成。负载均衡由 Scheduler 后续做。

Q: "RegionEpoch 两个版本号的区别？"
A: ConfVer 追踪 Peer 配置变更，Version 追踪 Key 范围变更。用于拒绝过时请求。

---

# Project 3C: Scheduler Balance

## 一、是什么

**功能**：调度器，管理集群元数据和负载均衡。

**解决的问题**：
- 记录 Region 分布
- 平衡各 Store 的 Region 数量
- 处理 Region 心跳

## 二、怎么做

### 心跳处理流程

```
┌─────────────────────────────────────────────────────────┐
│ ① Store 定时发送 Region 心跳                              │
│    RegionHeartbeat{Region, LeaderPeer}                   │
└─────────────────────────────────────────────────────────┘
                    │
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ② processRegionHeartbeat                                 │
│    检查 Region 是否已存在                                  │
│    ├─ 同 ID 存在 → 比较 Epoch，旧的拒绝                    │
│    └─ 不存在 → 扫描 Key 范围重叠 Region                    │
│              → 比较 Epoch，旧的拒绝                        │
└─────────────────────────────────────────────────────────┘
                    │
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ③ 更新记录                                                │
│    regions[regionID] = region                            │
│    stores[storeID].RegionCount++                         │
└─────────────────────────────────────────────────────────┘
```

### 均衡调度流程

```
┌─────────────────────────────────────────────────────────┐
│ ① 筛选可用 Store                                          │
│    在线 && 停机时间未超限                                   │
└─────────────────────────────────────────────────────────┘
                    │
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ② 按 Region 数量排序                                       │
│    stores[0] 最少，stores[n] 最多                         │
└─────────────────────────────────────────────────────────┘
                    │
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ③ 从最多 Store 找可搬 Region                              │
│    优先级：Pending > Follower > Leader                    │
│    ├─ Pending Region → 选它，搬                           │
│    ├─ Follower Region → 选它，搬                          │
│    ├─ Leader Region → 选它，搬                            │
│    └─ 都没有 → 试下一个 Store                             │
└─────────────────────────────────────────────────────────┘
                    │
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ④ 选择目标 Store                                          │
│    选 Region 最少的 Store                                 │
└─────────────────────────────────────────────────────────┘
                    │
                    ↓
┌─────────────────────────────────────────────────────────┐
│ ⑤ 生成 Operator                                           │
│    AddPeer(toStore) + RemovePeer(fromStore)              │
└─────────────────────────────────────────────────────────┘
```

## 三、为什么

### 为什么 Pending Region 优先？

```
Pending：正在迁移的 Region
  已完成部分步骤，但未完成全部
  
如果不优先：
  可能选其他 Region 搬迁
  → Pending Region 长时间未完成
  → 中间状态占用资源

优先处理：
  先完成 Pending → 再搬其他
  → 避免"半途而废"
```

### 为什么 Follower > Leader？

```
搬 Follower：
  不触发选举
  → 对客户端影响小
  
搬 Leader：
  触发选举
  → 客户端需要切换连接
  → 有短暂不可用

优先级：
  Pending（避免半途而废）
  > Follower（影响小）
  > Leader（影响大）
```

## 四、面试要点

Q: "心跳处理怎么防止过时信息？"
A: 用 RegionEpoch 版本号比较，旧版本拒绝。Split 产生新 Region ID，需扫描 Key 范围找重叠 Region。

Q: "Region 搬迁优先级？"
A: Pending > Follower > Leader。Pending 优先避免半途而废，Follower 比 Leader 影响小。

---

# Project 4: MVCC + Transaction

## 一、是什么

**功能**：实现 Percolator 模式的分布式事务。

**解决的问题**：
- 多版本并发控制（MVCC）
- 跨节点事务原子性（2PC）
- 快照隔离

## 二、怎么做

### MVCC 三 CF 结构

```
Lock CF:
  key: user_key（不带时间戳）
  value: {primary, startTs, kind, ttl}
  
  特点：同一 key 只能有一个锁

Default CF:
  key: EncodeKey(user_key, startTs)
  value: 用户数据
  
  特点：多版本，startTs 标识版本

Write CF:
  key: EncodeKey(user_key, commitTs)
  value: {kind, startTs}
  
  特点：commitTs 降序，作为版本索引
  kind: Put / Delete / Rollback
```

### 读取流程

```
GetValue(key, startTs):
    │
    ├─ ① 检查 Lock CF
    │     lock = GetLock(key)
    │     if lock != nil && lock.Ts <= startTs:
    │         return ErrLocked
    │
    ├─ ② 找 Write CF
    │     iter.Seek(EncodeKey(key, MaxTs))
    │     for iter.Valid():
    │         if decodeTs(iter.Key) <= startTs:
    │             write = ParseWrite(iter.Value)
    │             if write.Kind in [Put, Delete]:
    │                 break
    │         iter.Next()
    │
    └─ ③ 取 Default CF
          if write.Kind == Delete:
              return nil
          value = GetCF(CfDefault, EncodeKey(key, write.StartTs))
          return value
```

### 两阶段提交

```
┌─────────────────────────────────────────────────────────┐
│ Prewrite（第一阶段）                                       │
│                                                          │
│ ① 检查 Write CF                                           │
│    write = MostRecentWrite(key)                          │
│    if write.commitTs >= startTs:                         │
│        return ErrConflict                                │
│                                                          │
│ ② 检查 Lock CF                                            │
│    lock = GetLock(key)                                   │
│    if lock != nil:                                       │
│        return ErrLocked                                  │
│                                                          │
│ ③ 写入                                                    │
│    PutCF(CfDefault, EncodeKey(key, startTs), value)      │
│    PutCF(CfLock, key, {primary, startTs, kind})          │
└─────────────────────────────────────────────────────────┘
                    │ 成功
                    ↓
┌─────────────────────────────────────────────────────────┐
│ Commit（第二阶段）                                         │
│                                                          │
│ ① 检查 Write CF                                           │
│    write = CurrentWrite(key, startTs)                    │
│    if write.Kind == Rollback:                            │
│        return ErrRetryable                               │
│    if write != nil:                                      │
│        return nil  // 幂等                                │
│                                                          │
│ ② 检查 Lock CF                                            │
│    lock = GetLock(key)                                   │
│    if lock == nil:                                       │
│        return nil  // Prewrite 丢失                       │
│    if lock.Ts != startTs:                                │
│        return ErrRetryable                               │
│                                                          │
│ ③ 写入                                                    │
│    PutCF(CfWrite, EncodeKey(key, commitTs), {kind, startTs})│
│    DeleteCF(CfLock, key)                                 │
└─────────────────────────────────────────────────────────┘
```

### 数据流图

```
事务 T1: Put("a", "v1"), Put("b", "v2"), startTs=100

Prewrite Phase:
    │
    ├─ 检查 "a"
    │   ├─ Write CF: 无冲突
    │   ├─ Lock CF: 无锁
    │   └─ 写入:
    │       Default CF: {key="a_100", value="v1"}
    │       Lock CF: {key="a" → {primary="a", Ts=100, kind=Put}}
    │
    ├─ 检查 "b"
    │   ├─ Write CF: 无冲突
    │   ├─ Lock CF: 无锁
    │   └─ 写入:
    │       Default CF: {key="b_100", value="v2"}
    │       Lock CF: {key="b" → {primary="a", Ts=100, kind=Put}}
    │
    ↓
Commit Phase:
    │
    ├─ 检查 "a" (primary)
    │   ├─ CurrentWrite: 无
    │   ├─ Lock: {Ts=100} ✓
    │   └─ 写入:
    │       Write CF: {key="a_150", value={kind=Put, startTs=100}}
    │       Delete Lock CF: {key="a"}
    │
    ├─ 检查 "b" (secondary)
    │   ├─ CurrentWrite: 无
    │   ├─ Lock: {Ts=100} ✓
    │   └─ 写入:
    │       Write CF: {key="b_150", value={kind=Put, startTs=100}}
    │       Delete Lock CF: {key="b"}
```

## 三、为什么

### 为什么需要两种锁（Latches + Lock CF）？

```
场景：两个线程同时 Prewrite 同一个 key

没有 Latches：
  t1: 线程A 检查 Lock → nil
  t2: 线程B 检查 Lock → nil  ← A还没写入
  t3: 线程A 写 Lock → {Ts=100}
  t4: 线程B 写 Lock → {Ts=200} ← 覆盖A的锁
  → A的锁丢失

有 Latches：
  t1: 线程A 获取 Latches[key]
  t2: 线程B 等待
  t3: 线程A 检查 → 写入 → 释放 Latches
  t4: 线程B 获取 Latches → 检查 → 发现锁 → 返回错误

Latches：本地并发锁
Lock CF：分布式事务锁
```

### 为什么 Rollback 要写标记？

```
场景：rollback 比某些 prewrite 先到达

并行发送：
  客户端并行发 prewrite 到节点 A、B、C
  
时序：
  t1: prewrite-A 成功
  t2: prewrite-B 失败 → 客户端决定 rollback
  t3: rollback 到达 A → 写 Rollback 标记
  t4: prewrite-C 到达（迟到）
  
没有 Rollback 标记：
  prewrite-C 成功 → 写 Lock
  → 事务已"回滚"，但 C 有锁 → 数据不一致
  
有 Rollback 标记：
  prewrite-C 检查 Write CF → 发现 Rollback
  → 立即放弃，不写 Lock
```

### 为什么 Rollback 标记不影响其他事务？

```
Rollback 标记：
  Write CF: {key="a_100", value={kind=Rollback, startTs=100}}
  
事务 T2 (startTs=200)：
  Prewrite 检查：
    扫描 Write CF → 发现 {startTs=100}
    100 != 200 → 不冲突，忽略
  
  GetValue：
    扫描 Write CF → 发现 Rollback → 继续往前找
    
只有 T1 自己的迟到请求会匹配 startTs=100
```

### 为什么 Commit 时 Prewrite 丢失返回 nil？

```
Prewrite 丢失 = 事务已失败

返回 Retryable：
  客户端无限重试已失败的事务
  
返回 nil：
  客户端认为"已完成"，开始新事务
  → 正常处理后续业务
```

## 四、面试要点

Q: "MVCC 怎么实现？"
A: 三 CF：Lock CF 存锁，Default CF 存数据，Write CF 存版本索引。读取时 Lock 检查 → Write 找版本 → Default 取值。

Q: "事务怎么保证？"
A: 两阶段提交。Prewrite 检冲突写数据加锁，Commit 写提交记录删锁。

Q: "为什么需要两种锁？"
A: Latches 本地并发锁，防止检查+写入间隙竞态。Lock CF 分布式事务锁，标记占用。

Q: "Rollback 标记作用？"
A: 防止迟到的 prewrite 在 rollback 后意外成功。MVCC 版本隔离，不影响其他事务。

---

# 面试实战演练

## 自测问题清单

### Tier 1：必须能流畅回答

1. Raft 怎么选举 Leader？
2. 日志怎么复制？
3. 写请求完整流程？（从 Client 到返回）
4. Follower 为什么也 Apply？
5. MVCC 怎么实现？
6. 事务两阶段提交流程？
7. Range 分片 vs Hash 分片？
8. Region Split 流程？

### Tier 2：能讲清楚设计决策

1. 为什么 CompactLog 走 Raft？
2. 为什么新 Peer 继承 StoreId？
3. 为什么需要两种锁（Latches + Lock CF）？
4. 为什么 Rollback 要写标记？
5. 为什么用随机超时避免分裂票？
6. Leader Transfer 怎么保证数据完整？

### Tier 3：对比分析

1. 和 Raft 论文区别？（Follower Apply）
2. 和 TiKV 区别？（简化版，设计一致）
3. 和 Kafka/RocketMQ 区别？（它们用 Raft 做元数据）

## 讲述模板

```
"我实现了 TinyKV，一个分布式 KV 存储...

【架构】计算存储分离 + Multi-Raft + MVCC 事务

【Raft】完整状态机。有个设计点：Follower 也 Apply，
这是 TiKV 的设计，好处是故障切换快、支持本地读。

【分片】Range 分片，Scan 友好。Split 只改元数据，
不搬数据，瞬间完成。负载均衡由 Scheduler 做。

【事务】Percolator 模式 2PC。Prewrite 检冲突，
Commit 写记录。有个细节：两种锁——Latches 本地并发，
Lock CF 分布式事务。

【亮点】遇到过 CompactLog 并发竞争，用双重检查解决...
```

## 复习建议

1. **每个项目画一张数据流图**
2. **每个项目写 3 个"为什么"**
3. **找人对练 Tier 1-2 问题**
4. **对比阅读 TiKV 设计文档**