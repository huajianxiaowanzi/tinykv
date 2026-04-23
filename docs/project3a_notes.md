# Project 3A - Leader Transfer & ConfChange 个人笔记

## 一、3A 要解决什么问题？

Raft 集群运行中需要两种运维操作：
1. **Leader Transfer（领导权禅让）**：将 Leader 角色转移到指定节点（如原 Leader 负载过高、需要停机维护）
2. **ConfChange（配置变更）**：动态增减集群节点（如扩容、缩容、替换故障节点）

两者都在 Raft 层实现，不涉及上层 raftstore。

---

## 二、Leader Transfer 完整流程

### 2.1 核心思路

Leader 不能直接"让位"，因为新 Leader 必须拥有所有已提交的日志。所以禅让的本质是：**让目标节点发起选举并赢下选举**。

```
客户端请求 TransferLeader(目标=Node3)
    │
    ▼
Leader 收到 MsgTransferLeader
    │
    ├─ 目标日志已最新？ → 发 MsgTimeoutNow → 目标立刻选举
    │
    └─ 目标日志落后？   → 先发 MsgAppend 同步日志
                          → 收到 AppendResponse 时检查
                          → 日志追平后再发 MsgTimeoutNow
    │
    ▼
目标节点收到 MsgTimeoutNow
    │
    ▼
调用 Step(MsgHup) → 立刻发起选举
    │
    ▼
目标节点赢得选举，成为新 Leader
```

### 2.2 消息流转详解

```
┌─────────────────────────────────────────────────────────┐
│ ① 上层发起 TransferLeader                                │
│   调用 Raft.Step(MsgTransferLeader{From: target})       │
└──────────────────────┬──────────────────────────────────┘
                       │
          ┌────────────┴────────────┐
          │ 目标节点是 Leader 自己？  │
          └────────────┬────────────┘
                Yes → 直接返回，无事发生
                No  ↓
┌─────────────────────────────────────────────────────────┐
│ ② Follower/Candidate 收到 MsgTransferLeader              │
│   不是 Leader，无法处理，转发给 Leader                     │
│   m.To = r.Lead; r.msgs = append(r.msgs, m)            │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│ ③ Leader 收到 MsgTransferLeader → handleTransferLeader() │
│                                                          │
│   检查 1: 发送者是否在集群中（Prs[m.From]）               │
│   检查 2: 是否已有禅让进行中                              │
│     - 同一目标 → 忽略（去重）                             │
│     - 不同目标 → 终止上一个，开始新的                      │
│                                                          │
│   设置: r.leadTransferee = m.From                        │
│         r.transferElapsed = 0                            │
│                                                          │
│   分支:                                                  │
│   ┌─ Match == LastIndex → 日志已同步，直接发 MsgTimeoutNow│
│   └─ Match < LastIndex  → 发 sendAppend 同步日志         │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│ ④ Leader 收到 AppendResponse → handleAppendResponse()    │
│                                                          │
│   检查: leadTransferee == m.From                         │
│         && Prs[m.From].Match == LastIndex()              │
│                                                          │
│   日志追平 → 发送 MsgTimeoutNow                           │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│ ⑤ 目标节点收到 MsgTimeoutNow → handleTimeoutNowRequest()  │
│                                                          │
│   检查: 自己是否还在集群中（Prs[r.id]）                   │
│   调用: Step(MsgHup) → 立刻发起选举                      │
│   （清空 electionElapsed，跳过随机超时，马上竞选）         │
└─────────────────────────────────────────────────────────┘
```

### 2.3 关键设计细节

**1. 禅让期间拒绝新提案**

```go
func (r *Raft) handlePropose(m pb.Message) {
    r.appendEntry(m.Entries)
    if r.leadTransferee != None {
        return  // 禅让中，拒绝客户端请求
    }
    // ... 正常广播逻辑
}
```

原因：如果禅让期间继续接受提案，目标节点的日志会持续落后，永远追不上，禅让永远完不成。

**2. 禅让超时中止**

```go
func (r *Raft) leaderTick() {
    if r.leadTransferee != None {
        r.transferElapsed++
        if r.transferElapsed >= r.electionTimeout {
            r.leadTransferee = None  // 超时，放弃禅让
        }
    }
}
```

原因：目标节点可能已宕机或网络分区，无限等待会卡死集群。超时后恢复接受客户端请求。

**3. 角色转换时清除 leadTransferee**

```go
func (r *Raft) becomeFollower(term uint64, lead uint64) {
    // ...
    r.leadTransferee = None
}
```

原因：Leader 变成 Follower（如网络分区后收到更高 term），禅让自然终止。

**4. Follower/Candidate 转发禅让请求**

```go
case pb.MessageType_MsgTransferLeader:
    if r.Lead != None {
        m.To = r.Lead
        r.msgs = append(r.msgs, m)
    }
```

原因：只有 Leader 才能执行禅让，非 Leader 收到请求后转发给当前 Leader。

### 2.4 为什么用 MsgTimeoutNow 而不是直接降为 Follower？

直接 `becomeFollower()` 的问题是：
- 没有 Leader 领导的窗口期，集群不可用
- 目标节点不一定能赢下选举（可能同时有其他 Candidate）

`MsgTimeoutNow` 让目标节点**立刻发起选举**，用最小化窗口完成权力交接。目标节点拥有所有已提交日志 + 发起选举最早，赢下选举概率极高。

---

## 三、ConfChange（配置变更）

### 3.1 核心思路

ConfChange（AddNode/RemoveNode）走 Raft 日志共识，保证所有节点以相同顺序看到相同的配置变更。

```
客户端请求 AddNode(4) / RemoveNode(3)
    │
    ▼
Leader: Propose(EntryConfChange)
    │
    ▼
Raft 复制 → 多数确认 → Commit
    │
    ▼
所有节点 Apply 时调用:
  AddNode → addNode(id)
  RemoveNode → removeNode(id)
```

### 3.2 addNode 详解

```go
func (r *Raft) addNode(id uint64) {
    if _, ok := r.Prs[id]; !ok {
        r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1}
        r.PendingConfIndex = None
    }
}
```

**关键点**：

- `Next = LastIndex() + 1`：新节点从 Leader 最新日志的下一个位置开始，后续 Leader 会通过心跳/追加发现它落后，再发送快照或日志补齐
- `PendingConfIndex = None`：清除配置变更锁，允许下一次 ConfChange

### 3.3 removeNode 详解

```go
func (r *Raft) removeNode(id uint64) {
    if _, ok := r.Prs[id]; ok {
        delete(r.Prs, id)

        // 移除节点会降低多数派门槛，可能导致之前无法提交的日志现在可以提交
        if r.State == StateLeader && r.maybeCommit() {
            r.broadcastAppendEntry()
        }
    }
    r.PendingConfIndex = None
}
```

**核心逻辑**：

```
移除节点前: Prs = {1,2,3}，多数 = 2，需要 2 个确认
移除节点后: Prs = {1,2}，  多数 = 2，仍然需要 2 个确认

更典型的场景:
移除前: Prs = {1,2,3,4,5}，多数 = 3
移除后: Prs = {1,2,3,4}，  多数 = 3  (不变)
但是！3已经从分母中去掉了，如果 match 分布恰好变化，可能推进 commit

极端场景:
5 节点 → 3 节点：多数从 3 降到 2！
之前需要 3 确认，现在只需 2 确认，commit 可能立刻推进
```

### 3.4 PendingConfIndex — 配置变更互斥锁

```
Propose ConfChange → PendingConfIndex = entry.Index（上锁）
    ↓
Apply ConfChange → addNode/removeNode → PendingConfIndex = None（解锁）
    ↓
可以接受下一个 ConfChange
```

**为什么需要互斥？**

Raft 论文指出：一次只能有一个未提交的 ConfChange。原因：
- 配置变更是基于当前集群成员的，连续两次变更可能导致多数派计算混乱
- 如果前一个 ConfChange 未提交就接受新的，回滚时无法确定应该回到哪个配置

### 3.5 Follower 也要维护 Prs

| 角色 | 用 Prs 的 key | 用 Prs 的 value |
|------|-------------|----------------|
| Leader | 知道集群成员 | 追踪 Match/Next 复制进度 |
| Follower | 知道集群成员（投票判断） | 不使用 |

原因：
- 投票时只给配置内的节点投票
- Follower 随时可能成为 Leader，需要完整的 Prs
- `addNode/removeNode` 是所有节点 Apply 时都调用的

---

## 四、Match vs Committed — 易混淆概念

| 概念 | 含义 | 谁决定 |
|------|------|--------|
| `Match` | Follower 日志**持久化**到哪了 | Follower 追加成功后回复，Leader 记录 |
| `committed` | 日志**生效（Apply）**到哪了 | Leader 计算多数派后通知 |

```
Leader 视角某 Follower:
日志: [1] [2] [3] [4] [5] [6] [7] [8]
                       ↑           ↑       ↑
                 committed=5   Match=7  Leader last=8

Match >= committed 恒成立
Match 可以远大于 committed（日志先复制，后续再 commit）
```

**一句话：`Match` 是"写到了哪"，`committed` 是"生效到了哪"**

---

## 五、代码改动清单

| 位置 | 改动 | 说明 |
|------|------|------|
| `flowerStep` | 新增 MsgTransferLeader 转发、MsgTimeoutNow 处理 | Follower 处理禅让相关消息 |
| `candidateStep` | 新增 MsgTransferLeader 转发 | Candidate 也要转发禅让请求 |
| `leaderStep` | 实现 MsgTransferLeader 处理 | 调用 handleTransferLeader |
| `handleTransferLeader` | **新增函数** | 禅让核心逻辑：检查、设置 leadTransferee、判断日志同步 |
| `handleTimeoutNowRequest` | **新增函数** | 收到 MsgTimeoutNow 后立刻发起选举 |
| `sendTimeoutNow` | 实现发送逻辑 | 发送 MsgTimeoutNow 消息 |
| `handleAppendResponse` | 新增禅让续接逻辑 | 日志追平后继续禅让 |
| `handlePropose` | 新增禅让中拒绝提案 | `leadTransferee != None` 时 return |
| `leaderTick` | 新增禅让超时检测 | `transferElapsed >= electionTimeout` 时中止 |
| `becomeFollower` | 清除 leadTransferee | 角色变更时终止禅让 |
| `addNode` | 实现节点添加 | 创建 Progress、清除 PendingConfIndex |
| `removeNode` | 实现节点移除 | 删除 Prs 条目、maybeCommit、清除 PendingConfIndex |
| `appendEntry` | 设置 PendingConfIndex | ConfChange 日志上锁 |

---

## 六、面试可能的追问

**Q: Leader Transfer 为什么不直接让 Leader 退位？**
A: 直接退位会产生无 Leader 窗口期。MsgTimeoutNow 让目标节点立刻选举，最小化不可用时间。且目标节点拥有完整日志，赢下选举概率最高。

**Q: 禅让期间集群还能服务吗？**
A: 不能写入。`handlePropose` 在 `leadTransferee != None` 时拒绝新提案。但可以读（如果实现的是 Leader Read）。超时后会中止禅让恢复正常。

**Q: RemoveNode 后 commit 可能推进，为什么？**
A: 移除节点减少了 `len(Prs)`，多数派门槛可能降低。`maybeCommit()` 基于 `len(Prs)` 计算中位数，节点数减少意味着更容易达到多数派。

**Q: 为什么 ConfChange 要走 Raft 日志？**
A: 保证所有节点以相同顺序看到相同的配置变更。如果每个节点独立修改配置，可能出现不同节点看到不同的集群成员，导致分裂。
