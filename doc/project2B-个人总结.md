# Project 2B 个人总结

> **完成时间**：2026-04-21  
> **项目**：TinyKV - 基于 Raft 的分布式 KV 存储系统  
> **模块**：Fault-Tolerant KV on Raft（2B）

---

## 一、Project 2B 概述

### 1.1 目标

在 Project 2A（Raft 共识算法）的基础上，实现**基于 Multi-Raft 的分布式 KV 存储系统**，核心是构建 **RaftStore 层**，连接 Raft 共识与 KV 存储。

### 1.2 核心挑战

| 挑战 | 说明 |
|------|------|
| **Multi-Raft 架构** | 每个 Region 是一个独立的 Raft Group，需要管理多个 Peer |
| **消息路由** | Raft 消息需要正确路由到对应 Region 的 Peer |
| **日志 Apply** | 将 Raft committed entries 应用到 KV 存储 |
| **状态持久化** | Raft 状态、KV 数据都需要可靠持久化 |
| **并发控制** | 多 Peer 并发运行，需要保证线程安全 |

---

## 二、核心架构

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        Client                                    │
│                    (gRPC Request)                                │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                     RaftStorage                                  │
│            (接收请求，打包成 RaftCmdRequest)                      │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                      RaftStore                                   │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │  Region 1   │  │  Region 2   │  │  Region 3   │              │
│  │   Peer      │  │   Peer      │  │   Peer      │              │
│  │  (Raft)     │  │  (Raft)     │  │  (Raft)     │              │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                        Badger KV                                 │
│                   (持久化存储)                                    │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 核心组件

| 组件 | 文件 | 职责 |
|------|------|------|
| **Peer** | `peer.go` | 单个 Region 的 Raft 节点，包含 Raft 状态机和 Apply 逻辑 |
| **PeerMsgHandler** | `peer_msg_handler.go` | 处理 Peer 的消息（Raft 消息、RaftCmd、Tick 等） |
| **RaftWorker** | `raft_worker.go` | 处理 Raft 命令和日志 Apply 的工作线程 |
| **Router** | `router.go` | 消息路由，将消息发送到对应的 Peer |
| **Store** | `raftstore.go` | Store 级别的协调，管理所有 Peer |
| **Snapshot** | `snap/` | Raft 快照的生成、传输、加载 |

---

## 三、核心流程

### 3.1 Write 请求处理流程

```
Client: Put("foo", "bar")
         ↓
┌─────────────────────────────────────────────────────────┐
│ 1. RaftStorage.Write()                                   │
│    - 创建 Callback（带 done channel）                    │
│    - 打包成 RaftCmdRequest                               │
│    - 发送到 RaftRouter                                   │
└─────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────┐
│ 2. RaftstoreRouter.SendRaftCommand()                     │
│    - 包装成 MsgRaftCmd                                   │
│    - 发送到 router.peerSender (缓冲 40960)               │
└─────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────┐
│ 3. raftWorker.run() 从 raftCh 接收                        │
│    - 批量接收消息                                        │
│    - 按 RegionID 分组                                     │
│    - 发送到对应 Peer 的 HandleMsg()                        │
└─────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────┐
│ 4. peerMsgHandler.proposeRaftCommand()                   │
│    - 检查 Leader、Term、RegionEpoch                      │
│    - 保存 proposal 到列表（index, term, cb）              │
│    - RaftGroup.Propose(data)                            │
└─────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────┐
│ 5. Raft 内部处理                                          │
│    - 创建 Entry {Term, Index, Data}                      │
│    - 发送 MsgAppend 给 Follower                          │
│    - 等待过半数确认 → Committed                          │
└─────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────┐
│ 6. HandleRaftReady()                                     │
│    - SaveReadyState(rd) → 持久化到 raftdb                │
│    - SendMessages(rd.Messages)                           │
│    - ApplyCommittedEntries(rd.CommittedEntries)          │
└─────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────┐
│ 7. applyCommittedEntries()                               │
│    - execWriteRequest(cmd) → 写入 Badger                 │
│    - onRaftBaseResp() → 查找 proposal 并回调              │
└─────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────┐
│ 8. Callback.Done()                                       │
│    - cb.done <- struct{}{} → 唤醒等待的协程              │
└─────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────┐
│ 9. RaftStorage.Write() 返回                              │
│    - cb.WaitResp() → 从 done channel 接收                │
│    - return cb.Resp → 返回给客户端                       │
└─────────────────────────────────────────────────────────┘
```

### 3.2 Proposal 机制

**核心数据结构**：
```go
type proposal struct {
    index uint64        // 日志在 Raft 中的位置
    term  uint64        // 任期号
    cb    *Callback     // 客户端回调
}
```

**匹配逻辑**：
```go
func (d *peerMsgHandler) onRaftBaseResp(resp *raft_cmdpb.RaftCmdResponse, index uint64, term uint64) {
    for i, p := range d.proposals {
        if p.index == index && p.term == term {
            p.cb.Done(resp)              // 触发回调
            d.proposals = d.proposals[i+1:]
            break
        }
    }
}
```

**设计要点**：
- 通过 `index + term` 唯一标识每个请求
- Leader 变更后，过期的 proposal 会被清理（NotifyStaleReq）
- 客户端超时重试时，旧的 proposal 会被正确处理

---

## 四、关键技术点

### 4.1 双数据库设计

```
Engines 结构：
┌──────────────────────────────────────────────────────┐
│  Kv (badger.DB)                                      │
│  - 存储业务 KV 数据                                     │
│  - Key 格式：${cf}_${key}（模拟 Column Family）       │
│  - CF: default, write, lock                          │
├──────────────────────────────────────────────────────┤
│  Raft (badger.DB)                                    │
│  - 存储 Raft 元数据                                     │
│  - Key 格式：${regionId}_raftLocalState              │
│              ${regionId}_raftLog_${index}            │
│              ${regionId}_raftApplyState              │
└──────────────────────────────────────────────────────┘
```

**为什么分离**：
- **隔离性**：Raft 日志 GC 不影响业务数据
- **性能**：Raft 日志频繁写入，KV 数据读写分离
- **清晰性**：职责分离，便于维护和调试

### 4.2 Column Family 模拟

Badger 原生不支持 Column Family，TinyKV 通过 **Key 前缀**实现：

```go
func KeyWithCF(cf string, key []byte) []byte {
    return []byte(fmt.Sprintf("%s_%s", cf, key))
}

// 示例
GetCF(db, "default", "foo")  →  实际查询的 Key = "default_foo"
GetCF(db, "write", "foo")    →  实际查询的 Key = "write_foo"
GetCF(db, "lock", "foo")     →  实际查询的 Key = "lock_foo"
```

### 4.3 并发安全设计

#### 消息驱动模型

```
所有 Peer 操作通过消息触发：
┌──────────────────────────────────────────────────────┐
│  raftWorker 单线程处理消息                             │
│                                                      │
│  for msg := range peerSender {                       │
│      peerMsgHandler.HandleMsg(msg)                   │
│  }                                                   │
│                                                      │
│  好处：同一 Peer 的消息串行处理，无需额外锁             │
└──────────────────────────────────────────────────────┘
```

#### storeMeta 的锁保护

```go
type storeMeta struct {
    sync.RWMutex
    regionRanges *btree.BTree      // Region 范围查询
    regions      map[uint64]*Region // RegionID 映射
}

// Split 时更新
storeMeta.Lock()
storeMeta.regionRanges.Delete(&regionItem{region: oldRegion})
oldRegion.EndKey = split.SplitKey
storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: oldRegion})
storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
storeMeta.Unlock()
```

#### 深拷贝避免并发修改

```go
// 发送 Region 心跳时
clonedRegion := new(metapb.Region)
err := util.CloneMsg(p.Region(), clonedRegion)
// proto.Marshal + Unmarshal 实现深拷贝

// 原因：防止发送过程中 Region 被 Split 修改
```

---

## 五、核心请求处理逻辑

2B 的核心是实现 RaftStore 层，负责处理三类请求：
1. **普通请求**（Get/Put/Delete/Snap）- 由 `processRequest` 处理
2. **Admin 请求**（CompactLog/Split）- 由 `processAdminRequest` 处理
3. **配置变更**（AddNode/RemoveNode）- 由 `processConfChange` 处理

---

### 5.1 processRequest - 普通请求处理

**函数签名**：
```go
func (d *peerMsgHandler) processRequest(
    entry *pb.Entry, 
    requests *raft_cmdpb.RaftCmdRequest, 
    kvWB *engine_util.WriteBatch,
) *engine_util.WriteBatch
```

**处理流程**：
```
┌─────────────────────────────────────────────────────────┐
│ 1. 创建响应对象                                          │
│    resp := &raft_cmdpb.RaftCmdResponse{...}             │
└─────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────┐
│ 2. 遍历所有请求（一个 RaftCmdRequest 可包含多个请求）     │
│    for _, req := range requests.Requests { ... }        │
└─────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────┐
│ 3. 根据 CmdType 分发处理                                  │
│    - CmdType_Get    → 读取数据                          │
│    - CmdType_Put    → 写入数据                          │
│    - CmdType_Delete → 删除数据                          │
│    - CmdType_Snap   → 刷新写入 + 返回 Region 元数据       │
└─────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────┐
│ 4. 回调响应                                              │
│    d.handleProposal(entry, resp)                        │
│    return kvWB                                          │
└─────────────────────────────────────────────────────────┘
```

---

#### 5.1.1 CmdType_Get - 读取请求

**逻辑**：
```go
case raft_cmdpb.CmdType_Get:
    // 1. 检查 Key 是否在 Region 范围内
    key := req.Get.Key
    if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
        BindRespError(resp, err)  // 返回错误：Key 不在本 Region
    } else {
        // 2. Get 请求需要先刷新写入（先写再读）
        kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
        kvWB = &engine_util.WriteBatch{}
        
        // 3. 从 Badger KV 数据库读取数据
        value, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, req.Get.Cf, req.Get.Key)
        
        // 4. 构造响应
        resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
            CmdType: raft_cmdpb.CmdType_Get,
            Get:     &raft_cmdpb.GetResponse{Value: value},
        })
    }
```

**关键点**：
| 设计点 | 说明 |
|--------|------|
| Key 范围检查 | `CheckKeyInRegion` 确保 Key 在 `[StartKey, EndKey)` 内 |
| 先写再读 | `MustWriteToDB()` 刷新积压的写入，确保读一致性 |
| Column Family | `GetCF` 从指定 CF 读取（default/write/lock） |

---

#### 5.1.2 CmdType_Put - 写入请求

**逻辑**：
```go
case raft_cmdpb.CmdType_Put:
    // 1. 检查 Key 是否在 Region 范围内
    key := req.Put.Key
    if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
        BindRespError(resp, err)
    } else {
        // 2. 写入 WriteBatch（累积，不立即落盘）
        kvWB.SetCF(req.Put.Cf, req.Put.Key, req.Put.Value)
        
        // 3. 构造响应
        resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
            CmdType: raft_cmdpb.CmdType_Put,
            Put:     &raft_cmdpb.PutResponse{},
        })
    }
```

**关键点**：
| 设计点 | 说明 |
|--------|------|
| Key 范围检查 | 确保写入的 Key 属于本 Region 管辖 |
| WriteBatch 累积 | 多个 Put/Delete 累积到 kvWB，批次结束时统一落盘 |
| Column Family | `SetCF` 写入指定 CF（如 `default_foo`） |

---

#### 5.1.3 CmdType_Delete - 删除请求

**逻辑**：
```go
case raft_cmdpb.CmdType_Delete:
    // 1. 检查 Key 是否在 Region 范围内
    key := req.Delete.Key
    if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
        BindRespError(resp, err)
    } else {
        // 2. 写入删除标记到 WriteBatch
        kvWB.DeleteCF(req.Delete.Cf, req.Delete.Key)
        
        // 3. 构造响应
        resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
            CmdType: raft_cmdpb.CmdType_Delete,
            Delete:  &raft_cmdpb.DeleteResponse{},
        })
    }
```

**关键点**：
| 设计点 | 说明 |
|--------|------|
| Key 范围检查 | 同 Put |
| DeleteCF | 写入空 Entry（Badger 中标记删除） |
| 批量删除 | 同 Put，累积到 kvWB 统一落盘 |

---

#### 5.1.4 CmdType_Snap - 快照请求（测试专用）

**逻辑**：
```go
case raft_cmdpb.CmdType_Snap:
    // 1. 检查 Region Epoch 版本
    if requests.Header.RegionEpoch.Version != d.Region().RegionEpoch.Version {
        BindRespError(resp, &util.ErrEpochNotMatch{})
    } else {
        // 2. 刷新写入
        kvWB.MustWriteToDB(d.peerStorage.Engines.Kv)
        kvWB = &engine_util.WriteBatch{}
        
        // 3. 返回 Region 元数据
        resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
            CmdType: raft_cmdpb.CmdType_Snap,
            Snap:    &raft_cmdpb.SnapResponse{Region: d.Region()},
        })
    }
```

**关键点**：
| 设计点 | 说明 |
|--------|------|
| Epoch 检查 | 确保 Region 版本一致，防止读取过期元数据 |
| 刷新写入 | 同 Get，确保后续扫描能看到最新数据 |
| 测试专用 | 不是 Raft 快照，是测试框架用于验证数据的后门命令 |

---

### 5.2 processAdminRequest - Admin 请求处理

**函数签名**：
```go
func (d *peerMsgHandler) processAdminRequest(
    entry *pb.Entry, 
    requests *raft_cmdpb.RaftCmdRequest, 
    kvWB *engine_util.WriteBatch,
) *engine_util.WriteBatch
```

**处理流程**：
```
┌─────────────────────────────────────────────────────────┐
│ 1. 获取 AdminRequest                                     │
│    adminReq := requests.AdminRequest                    │
└─────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────┐
│ 2. 根据 CmdType 分发处理                                  │
│    - AdminCmdType_CompactLog → 日志截断                 │
│    - AdminCmdType_Split      → 区域分裂                 │
└─────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────┐
│ 3. 执行具体逻辑                                          │
│    - CompactLog: 更新 TruncatedState，调度 GC 任务         │
│    - Split: 4 层验证，更新 storeMeta，创建新 Peer           │
└─────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────┐
│ 4. 回调响应（Split 需要，CompactLog 不需要）               │
│    d.handleProposal(entry, resp)                        │
└─────────────────────────────────────────────────────────┘
```

---

#### 5.2.1 AdminCmdType_CompactLog - 日志截断

**逻辑**：
```go
case raft_cmdpb.AdminCmdType_CompactLog:
    // 1. 防御性检查：只接受更新的 CompactIndex
    if adminReq.CompactLog.CompactIndex > d.peerStorage.applyState.TruncatedState.Index {
        // 2. 更新 TruncatedState
        truncatedState := d.peerStorage.applyState.TruncatedState
        truncatedState.Index = adminReq.CompactLog.CompactIndex
        truncatedState.Term = adminReq.CompactLog.CompactTerm
        
        // 3. 调度 GC 任务到 raftlog-gc worker
        d.ScheduleCompactLog(adminReq.CompactLog.CompactIndex)
        
        // 4. 日志记录
        log.Infof("%d apply commit, entry %v, type %s, truncatedIndex %v", 
            d.peer.PeerId(), entry.Index, adminReq.CmdType, adminReq.CompactLog.CompactIndex)
    }
```

**关键点**：
| 设计点 | 说明 |
|--------|------|
| 防御性检查 | `CompactIndex > TruncatedState.Index` 防止回退 |
| TruncatedState | 记录最后一条被截断日志的 Index 和 Term |
| ScheduleCompactLog | 发送 `RaftLogGCTask` 到 worker，异步删除物理日志 |
| 不需要回调 | CompactLog 是内部管理操作，没有客户端等待结果 |

**数据流**：
```
CompactLog(Index=30)
    ↓
TruncatedState.Index = 30
    ↓
ScheduleCompactLog(30) → 发送任务 {StartIdx: 0, EndIdx: 31}
    ↓
raftlog-gc worker 删除 [0, 31) 范围的日志
```

---

#### 5.2.2 AdminCmdType_Split - 区域分裂

**逻辑**：
```go
case raft_cmdpb.AdminCmdType_Split:
    // ========== 第一阶段：4 层验证 ==========
    
    // 1. RegionId 检查
    if requests.Header.RegionId != d.regionId {
        regionNotFound := &util.ErrRegionNotFound{RegionId: requests.Header.RegionId}
        d.handleProposal(entry, ErrResp(regionNotFound))
        return kvWB
    }
    
    // 2. Epoch 版本检查
    if errEpochNotMatch, ok := util.CheckRegionEpoch(requests, d.Region(), true).(*util.ErrEpochNotMatch); ok {
        d.handleProposal(entry, ErrResp(errEpochNotMatch))
        return kvWB
    }
    
    // 3. SplitKey 范围检查
    if err := util.CheckKeyInRegion(adminReq.Split.SplitKey, d.Region()); err != nil {
        d.handleProposal(entry, ErrResp(err))
        return kvWB
    }
    
    // 4. Peer 数量检查
    if len(d.Region().Peers) != len(adminReq.Split.NewPeerIds) {
        d.handleProposal(entry, ErrRespStaleCommand(d.Term()))
        return kvWB
    }
    
    // ========== 第二阶段：执行分裂 ==========
    
    oldRegion, split := d.Region(), adminReq.Split
    oldRegion.RegionEpoch.Version++                          // 5. 更新版本号
    newRegion := d.createNewSplitRegion(split, oldRegion)    // 6. 创建新 Region
    
    // 7. 更新 storeMeta（加锁）
    storeMeta := d.ctx.storeMeta
    storeMeta.Lock()
    storeMeta.regionRanges.Delete(&regionItem{region: oldRegion})
    oldRegion.EndKey = split.SplitKey
    storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: oldRegion})
    storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
    storeMeta.regions[newRegion.Id] = newRegion
    storeMeta.Unlock()
    
    // 8. 持久化 Region 状态
    meta.WriteRegionState(kvWB, oldRegion, rspb.PeerState_Normal)
    meta.WriteRegionState(kvWB, newRegion, rspb.PeerState_Normal)
    
    // 9. 重置统计
    d.SizeDiffHint = 0
    d.ApproximateSize = new(uint64)
    
    // 10. 创建并启动新 Peer
    peer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.schedulerTaskSender, d.ctx.engine, newRegion)
    if err != nil {
        log.Panic(err)
    }
    d.ctx.router.register(peer)
    d.ctx.router.send(newRegion.Id, message.Msg{Type: message.MsgTypeStart})
    
    // 11. 回调响应
    d.handleProposal(entry, &raft_cmdpb.RaftCmdResponse{
        Header: &raft_cmdpb.RaftResponseHeader{},
        AdminResponse: &raft_cmdpb.AdminResponse{
            CmdType: raft_cmdpb.AdminCmdType_Split,
            Split:   &raft_cmdpb.SplitResponse{Regions: []*metapb.Region{newRegion, oldRegion}},
        },
    })
    
    // 12. 发送心跳给 Scheduler（仅 Leader）
    if d.IsLeader() {
        d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
        d.notifyHeartbeatScheduler(newRegion, peer)
    }
```

**关键点**：
| 设计点 | 说明 |
|--------|------|
| 4 层验证 | RegionId、Epoch、SplitKey、Peer 数量，防止过期/错误请求 |
| Version++ | 数据范围变更，递增 RegionEpoch.Version |
| StoreId 继承 | 新 Peer 继承旧 Peer 的 StoreId，无需数据搬迁 |
| storeMeta 更新 | B-Tree 管理范围，Map 管理 ID，需要加锁 |
| 新 Peer 启动 | 通过 `MsgTypeStart` 消息驱动启动 |
| 心跳上报 | Split 后立即通知 Scheduler 更新集群拓扑 |

**数据流**：
```
Split Request (SplitKey="m")
    ↓
4 层验证通过
    ↓
oldRegion: ["", "z") → ["", "m")  Version++
newRegion: ["m", "z")  Version=1
    ↓
更新 storeMeta + 持久化
    ↓
创建新 Peer + 启动
    ↓
发送心跳给 Scheduler
```

---

### 5.3 processConfChange - 配置变更处理

**函数签名**：
```go
func (d *peerMsgHandler) processConfChange(
    entry *pb.Entry, 
    cc *pb.ConfChange, 
    kvWB *engine_util.WriteBatch,
) *engine_util.WriteBatch
```

**处理流程**：
```
┌─────────────────────────────────────────────────────────┐
│ 1. 反序列化 RaftCmdRequest                                │
│    msg.Unmarshal(cc.Context)                            │
└─────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────┐
│ 2. Epoch 验证（防止重复请求）                             │
│    CheckRegionEpoch(msg, region, true)                  │
└─────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────┐
│ 3. 根据 ChangeType 分发处理                               │
│    - ConfChangeType_AddNode    → 添加 Peer               │
│    - ConfChangeType_RemoveNode → 移除 Peer              │
└─────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────┐
│ 4. 更新 Region.Peers + ConfVer++                         │
└─────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────┐
│ 5. 持久化 + 更新缓存                                     │
│    meta.WriteRegionState(...)                           │
│    updateStoreMeta(...)                                 │
│    insertPeerCache(...) / removePeerCache(...)          │
└─────────────────────────────────────────────────────────┘
         ↓
┌─────────────────────────────────────────────────────────┐
│ 6. 回调响应                                              │
│    d.handleProposal(entry, resp)                        │
└─────────────────────────────────────────────────────────┘
```

---

#### 5.3.1 ConfChangeType_AddNode - 添加节点

**逻辑**：
```go
case pb.ConfChangeType_AddNode:
    log.Infof("[AddNode] %v add %v", d.PeerId(), cc.NodeId)
    
    // 1. 检查待添加的节点必须原先在 Region 中不存在
    if d.searchPeerWithId(cc.NodeId) == len(region.Peers) {
        // 2. Region 中追加新的 peer
        region.Peers = append(region.Peers, changePeerReq.Peer)
        
        // 3. 配置版本号 +1
        region.RegionEpoch.ConfVer++
        
        // 4. 持久化 Region 状态
        meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
        
        // 5. 更新 storeMeta 中的 region 信息
        d.updateStoreMeta(region)
        
        // 6. 更新 peerCache（peerId → Peer 映射）
        d.insertPeerCache(changePeerReq.Peer)
    }
```

**关键点**：
| 设计点 | 说明 |
|--------|------|
| 存在性检查 | `searchPeerWithId` 返回 `len(Peers)` 表示不存在 |
| ConfVer++ | 配置变更后递增 RegionEpoch.ConfVer |
| peerCache | 保存 `peerId → Peer` 映射，用于后续消息发送 |
| Epoch 验证 | 入口处已检查，防止重复执行 |

---

#### 5.3.2 ConfChangeType_RemoveNode - 移除节点

**逻辑**：
```go
case pb.ConfChangeType_RemoveNode:
    log.Infof("[RemoveNode] %v remove %v", d.PeerId(), cc.NodeId)
    
    // 1. 如果目标节点是自身，直接销毁并返回
    if cc.NodeId == d.PeerId() {
        d.destroyPeer()
        log.Infof("[RemoveNode] destroy %v completed", cc.NodeId)
        return kvWB
    }
    
    // 2. 待删除的节点必须存在于 region 中
    n := d.searchPeerWithId(cc.NodeId)
    if n != len(region.Peers) {
        // 3. 删除 RaftGroup 中的第 n 个 peer（数组索引）
        region.Peers = append(region.Peers[:n], region.Peers[n+1:]...)
        
        // 4. 配置版本号 +1
        region.RegionEpoch.ConfVer++
        
        // 5. 持久化 Region 状态
        meta.WriteRegionState(kvWB, region, rspb.PeerState_Normal)
        
        // 6. 更新 storeMeta 中的 region 信息
        d.updateStoreMeta(region)
        
        // 7. 更新 peerCache
        d.removePeerCache(cc.NodeId)
    }
```

**关键点**：
| 设计点 | 说明 |
|--------|------|
| 自毁逻辑 | 如果被移除的是自己，调用 `destroyPeer()` 销毁 Peer |
| 切片删除 | `append(Peers[:n], Peers[n+1:]...)` 删除索引为 n 的元素 |
| n 的含义 | `searchPeerWithId` 返回的是**数组索引**，不是 Peer ID |
| peerCache 清理 | 移除后需要清理缓存，避免后续发送消息失败 |

---

**辅助函数 searchPeerWithId**：
```go
// 根据 Peer ID 查找其在 Peers 数组中的索引
func (d *peerMsgHandler) searchPeerWithId(nodeId uint64) int {
    for id, peer := range d.peerStorage.region.Peers {
        if peer.Id == nodeId {
            return id  // 找到，返回索引
        }
    }
    return len(d.peerStorage.region.Peers)  // 没找到，返回长度
}
```

**优化建议**：
- 返回 `(int, bool)` 替代 `len(Peers)`，语义更清晰
- 符合 Go 标准库惯例（类似 map 查找）

---

### 5.4 三类请求的对比

| 特性 | processRequest | processAdminRequest | processConfChange |
|------|----------------|---------------------|-------------------|
| **处理请求** | Get/Put/Delete/Snap | CompactLog/Split | AddNode/RemoveNode |
| **验证检查** | Key 范围检查 | Split 有 4 层验证 | Epoch 验证 |
| **写入方式** | WriteBatch 累积 | 直接持久化 | 直接持久化 |
| **回调客户端** | 是 | Split 是，CompactLog 否 | 是 |
| **Epoch 变更** | 无 | Split: Version++ | ConfChange: ConfVer++ |
| **触发者** | 客户端 | Scheduler | Scheduler |

---

## 六、问题与解决

### 6.1 Windows 文件锁问题（测试失败）

**问题现象**：
```
panic: remove C:\Users\...\snap\gen_1_12_197.meta.tmp: 
The process cannot access the file because it is being used by another process.
```

**问题原因**：
Windows 与 Linux 的文件系统行为差异：
| 系统 | 文件打开时能否删除 |
|------|------------------|
| Linux/Unix | ✅ 可以（删除后文件句柄仍有效） |
| Windows | ❌ 不可以（文件被占用时无法删除） |

**根本原因**：快照生成流程中，文件句柄未正确关闭。

**问题代码**（`kv/raftstore/snap/snap.go`）：
```go
func (s *Snap) initForBuilding() error {
    // 打开 meta 临时文件
    file, err := os.OpenFile(s.MetaFile.TmpPath, os.O_CREATE|os.O_WRONLY, 0600)
    s.MetaFile.File = file  // 保存文件句柄
    ...
}

func (s *Snap) saveMetaFile() error {
    _, err = s.MetaFile.File.Write(bin)  // 写入
    err = os.Rename(s.MetaFile.TmpPath, s.MetaFile.Path)  // 重命名
    // ❌ 缺少：s.MetaFile.File.Close()
    ...
}
```

**修复方案**：在 `saveMetaFile()` 中写入后关闭文件句柄：
```go
func (s *Snap) saveMetaFile() error {
    _, err = s.MetaFile.File.Write(bin)
    if err != nil {
        return errors.WithStack(err)
    }
    // 新增：关闭文件句柄
    err = s.MetaFile.File.Close()
    if err != nil {
        return errors.WithStack(err)
    }
    err = os.Rename(s.MetaFile.TmpPath, s.MetaFile.Path)
    ...
}
```

**教训**：
1. Windows 开发需要注意文件句柄的生命周期管理
2. 跨平台代码需要在两种系统上都测试
3. `defer file.Close()` 是好习惯，但要注意执行时机

### 6.2 调试代码残留

**问题现象**：测试输出中一直打印 "5"。

**原因**：`raft/raft.go` 的 `handlePropose` 函数中有调试代码：
```go
fmt.Println(len(r.Prs))  // 打印集群节点数（5 个）
```

**解决**：删除调试代码。

**教训**：调试代码提交前需要清理，或改用条件编译。

---

### 6.3 深拷贝 vs 锁的选择

**问题**：Region 心跳发送时，Region 元数据可能被并发修改。

**方案对比**：
| 方案 | 优点 | 缺点 |
|------|------|------|
| 锁 | 无拷贝开销 | 可能阻塞、死锁风险 |
| 深拷贝 | 无阻塞、无死锁 | 序列化开销（~10μs） |

**选择深拷贝**：
- 锁只能保护读取瞬间，不能保护异步发送过程
- 网络传输本就需要序列化
- 心跳频率低（1 次/秒），开销可接受

### 6.2 Proposal 匹配问题

**问题**：如何确保 Apply 时找到正确的回调？

**解决**：
- `index + term` 唯一标识
- Leader 变更后清理过期 proposal
- 支持乱序 Apply（根据 index 匹配）

### 6.3 Region 路由并发安全

**问题**：Split 时更新路由表，同时有查询请求。

**解决**：
- `storeMeta.RWMutex` 保护
- 读写分离（Split 写锁，查询读锁）
- B-Tree 保证查询效率 O(log N)

---

## 七、简历亮点提炼

### 7.1 核心亮点

| 亮点 | 描述 | 技术深度 |
|------|------|----------|
| **Follower Apply 设计** | 突破标准 Raft 的 Leader-only Apply，支持快速故障切换和本地读取 | ⭐⭐⭐⭐⭐ |
| **Region Split 实现** | 完整的动态分裂机制，包括 4 层验证、路由表更新、新 Peer 启动 | ⭐⭐⭐⭐⭐ |
| **深拷贝并发设计** | 用 proto 序列化实现深拷贝，避免锁的并发问题 | ⭐⭐⭐⭐ |
| **CompactLog 防御性** | 防止日志 GC 回退，应对乱序请求 | ⭐⭐⭐ |

### 7.2 面试表述建议

```
面试官：说说你项目中印象最深的技术问题？

我：TinyKV 的 Follower Apply 设计是一个与标准 Raft 不同的亮点。

【标准做法】
标准 Raft 只有 Leader Apply，Follower 只复制日志不执行。

【我们的设计】
TinyKV 让所有节点（包括 Follower）都 Apply committed entry，
保持 KV 数据完全一致。

【收益】
1. 快速故障切换 - Follower 随时可以成为 Leader
2. 本地读取优化 - Follower 可以处理读请求
3. 状态一致 - 所有节点数据相同，简化调试

【代价】
Follower 多做了一些写操作，但 Badger 的批量写入性能很好，
这个开销可以接受。
```

---

## 八、学习收获

### 8.1 分布式系统

- **Raft 共识**：理解了 Leader 选举、日志复制、安全性保证
- **Multi-Raft**：多个 Raft Group 并行运行，支持水平扩展
- **自动分片**：Region Split 实现 Auto-Sharding，线性扩展

### 8.2 并发编程

- **消息驱动模型**：所有操作通过消息触发，并发安全
- **锁的粒度**：细粒度锁 vs 粗粒度锁的权衡
- **深拷贝**：用 CPU 换 correctness，避免死锁

### 8.3 存储引擎

- **Badger KV**：LSM-Tree 实现，支持事务
- **Column Family**：Key 前缀模拟 CF 隔离
- **WriteBatch**：批量写入，减少 IO

### 8.4 工程实践

- **防御性编程**：Epoch 检查、边界验证
- **错误处理**：ErrRegionNotFound、ErrEpochNotMatch
- **日志调试**：关键路径打点，便于问题排查

---

## 九、后续改进

### 9.1 代码优化

- `searchPeerWithId` 返回 `(int, bool)` 替代 `len(Peers)`
- 减少不必要的 `log.Panic`，使用错误返回
- 统一错误处理模式

### 9.2 性能优化

- 批量 Apply 时减少锁竞争
- Region 路由表缓存优化
- Snapshot 传输压缩

### 9.3 功能完善

- Region Merge（合并小 Region）
- Leader 主动转移
- 副本读写分离

---

## 十、参考资源

- [TinyKV 官方文档](https://github.com/pingcap-incubator/tinykv)
- [Raft 论文](https://raft.github.io/raft.pdf)
- [TiKV 设计文档](https://tikv.org/docs/4.0/concepts/overview/)
- [MIT 6.824 Lab](https://pdos.csail.mit.edu/6.824/)

---

**总结**：Project 2B 是 TinyKV 的核心模块，实现了基于 Multi-Raft 的分布式 KV 存储。通过这个项目，我深入理解了 Raft 共识算法、分布式系统设计、并发编程等核心技术，为后续学习更复杂的分布式系统打下了坚实基础。
