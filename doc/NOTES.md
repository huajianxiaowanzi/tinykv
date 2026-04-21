# TinyKV 学习笔记

> 每天记录：理解了什么、哪个"啊哈时刻"、遇到什么问题、明天的任务

---

## 📚 快速索引

- [架构概览](#架构概览)
- [Project 2A - Raft 选举](#project-2a---raft-选举)
- [Project 2B - 日志复制](#project-2b---日志复制)
- [Project 2C - 快照](#project-2c---快照)
- [Project 3 - Leader 转移与分裂](#project-3---leader-转移与分裂)
- [Project 4 - MVCC 事务](#project-4---mvcc-事务)
- [问题清单](#问题清单)
- [每日笔记](#每日笔记)

---

## 架构概览

### 三层架构

```
Client 层 (TinySQL / 测试用例)
    ↓ gRPC
Server 层 (kv/server/)
    ↓
Storage 层 (kv/storage/)
    ↓
Raft 层 (raft/) + RaftStore 层 (kv/raftstore/)
    ↓
Badger DB
```

### 四个 Project

| Project | 主题 | 关键文件 |
|---------|------|----------|
| 1 | Standalone KV | `kv/server/raw_api.go` |
| 2A | Raft 选举 | `raft/raft.go` |
| 2B | 日志复制 | `kv/raftstore/peer_msg_handler.go` |
| 2C | 快照 | `raft/storage.go` |
| 3 | Leader 转移、分裂 | `kv/raftstore/peer_msg_handler.go` |
| 4 | MVCC 事务 | `kv/transaction/mvcc/` |

---

## Project 2A - Raft 选举

### 核心概念

- **Follower**：被动接收 Leader 消息
- **Candidate**：发起选举
- **Leader**：处理所有写请求

### 状态转换图

```
Follower --(选举超时)--> Candidate --(获胜)--> Leader
               ↑                              ↓
               └──────────(心跳超时)──────────┘
```

### 关键变量

| 变量 | 含义 | 持久化 |
|------|------|--------|
| `Term` | 当前任期 | 是 |
| `Vote` | 投给谁 | 是 |
| `Lead` | 当前 Leader | 否 |
| `State` | Raft 状态 | 否 |

---

## Project 2B - 日志复制

### 核心调用链

```
Client 写请求
    ↓
RaftStorage.Write() → 创建 Callback
    ↓
RaftstoreRouter.SendRaftCommand() → 发送到 raftCh
    ↓
raftWorker.run() → 从 channel 接收
    ↓
peerMsgHandler.HandleMsg() → 分发
    ↓
proposeRaftCommand() → 保存 proposal
    ↓
Raft.Propose() → 复制日志
    ↓
HandleRaftReady() → 持久化 + Apply
    ↓
processCommittedEntry() → 执行 KV 操作 + 回调
    ↓
Callback.Done() → 唤醒客户端
```

### 关键数据结构

```go
// proposal - 保存待处理的请求
type proposal struct {
    index uint64        // 日志 Index
    term  uint64        // 任期号
    cb    *Callback     // 回调
}

// Callback - 用于同步等待
type Callback struct {
    Resp *raft_cmdpb.RaftCmdResponse
    done chan struct{}
}
```

### 三个指针

```
日志索引：  1    2    3    4    5    6    7
            ├────├────├────├────├────├────├────
            │    │         │              │
            applied       stabled        committed

- committed: 已提交的日志 (Raft 协议保证)
- stabled:   已持久化的日志 (写入磁盘)
- applied:   已应用的日志 (执行到状态机)
```

---

## Project 2C - 快照

### 为什么需要快照

- 日志无限增长会耗尽磁盘
- Follower 落后太多时，发送快照比发送日志更高效

### 快照触发条件

- 日志数量超过阈值
- Follower 的 NextIndex 小于已截断的日志

---

## Project 3 - Leader 转移与分裂

### Leader Transfer

- 当前 Leader 主动让位
- 用于负载均衡、优雅下线

### Region Split

```
分裂前: Region{1-100}
            ↓
分裂后: Region{1-50} + Region{51-100}
```

### ConfChange

- AddNode: 添加节点
- RemoveNode: 移除节点

---

## Project 4 - MVCC 事务

### 2PC 流程

```
1. Begin
    ↓
2. Prewrite (写 Primary + Secondaries)
    ↓
3. Commit (提交 Primary → 提交 Secondaries)
```

### MVCC 版本

```
Key = "foo"
  → Write CF: foo_T100 = {StartTs: 100, CommitTs: 150}
  → Default CF: foo_T100 = "value"
```

---

## 问题清单

### 待解决问题

- [ ] 
- [ ] 
- [ ] 

### 已解决问题

- [x] 
- [x] 

---

## 每日笔记

### 2024-01-XX - Project 2B 第一天

#### 📖 今天理解了什么

（在这里记录）

#### 💡 哪个"啊哈时刻"

（之前不懂，突然懂了的点）

#### 🐛 遇到什么问题

（测试失败、变量不理解、调用链断了）

#### ✅ 明天的任务

- [ ] 
- [ ] 
- [ ] 

---

### 2024-01-XX - Project 2B 第二天

#### 📖 今天理解了什么

#### 💡 哪个"啊哈时刻"

#### 🐛 遇到什么问题

#### ✅ 明天的任务

- [ ] 
- [ ] 
- [ ] 

---

## 测试命令速查

```powershell
# Project 2A
go test -v --count=1 ./raft -run "2A$"

# Project 2B
go test -v --count=1 ./kv/test_raftstore -run "2B$"

# Project 2C
go test -v --count=1 ./raft -run "2C$"
go test -v --count=1 ./kv/test_raftstore -run "2C$"

# 清理测试垃圾
rm -rf /tmp/*test-raftstore*
```

---

## 关键代码位置速查

| 功能 | 文件 | 函数 |
|------|------|------|
| 提出请求 | `kv/raftstore/peer_msg_handler.go` | `proposeRequest()` |
| 处理 Ready | `kv/raftstore/peer_msg_handler.go` | `HandleRaftReady()` |
| 持久化日志 | `kv/raftstore/peer_storage.go` | `Append()` |
| 应用日志 | `kv/raftstore/peer_msg_handler.go` | `processCommittedEntry()` |
| 回调触发 | `kv/raftstore/peer_msg_handler.go` | `handleProposal()` |
