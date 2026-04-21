# TinyKV 面试问题及回答

本文档整理了 TinyKV 项目中可以写入简历和在面试中提及的技术亮点，按 Problem-Solution 格式组织。

---

## 1. 深拷贝 vs 锁：并发场景下的数据传输设计

### 问题背景

在 Region 心跳发送模块中，Peer 需要定期向 Scheduler 发送 Region 元数据。但发现 Scheduler 有时会收到不一致的 Region 状态（如 EndKey 被修改）。

**根本原因**：Peer 在发送心跳的同时，Split 操作可能正在修改同一个 Region 对象。直接使用引用（`p.Region()`）导致数据在 channel 传输过程中被修改，Scheduler 收到脏数据。

### 方案对比

**方案 1：用锁保护**

```go
// ❌ 问题方案
p.mu.Lock()
region := p.Region()
p.mu.Unlock()
ch <- &SchedulerRegionHeartbeatTask{Region: region}
// 问题：发送的是引用，解锁后 region 仍可能被修改
```

**问题分析**：
- 锁只能保护"读取瞬间"，不能保护"异步发送过程"
- 如果持有锁等待发送完成，可能阻塞其他操作（如 Split、Apply）
- 极端情况下可能导致死锁

**方案 2：深拷贝**

```go
// ✅ 最终方案
clonedRegion := new(metapb.Region)
err := util.CloneMsg(p.Region(), clonedRegion)
ch <- &SchedulerRegionHeartbeatTask{Region: clonedRegion}
```

**实现细节**：
```go
func CloneMsg(origin, cloned proto.Message) error {
    data, err := proto.Marshal(origin)      // 序列化
    if err != nil { return err }
    return proto.Unmarshal(data, cloned)    // 反序列化
}
```

### 最终选择：深拷贝

**理由**：
1. **无并发风险**：发送的是独立副本，发送过程中不会被修改
2. **无死锁风险**：不需要持有锁等待发送完成
3. **代码简单**：一行代码，不易出错
4. **开销确定**：~10μs/次（1KB 数据），心跳频率 1 次/秒，完全可接受
5. **顺带收益**：网络传输本就需要序列化，深拷贝没有额外损失

### 核心设计思想

> **Ownership Transfer（所有权转移）**：跨线程/协程传输的数据必须独立拥有所有权，不能共享引用。

### 面试回答模板

```
面试官：说说你项目中遇到的并发问题？

我：有一个 Region 心跳发送时的并发一致性设计让我印象比较深。

【问题】
Peer 定期向 Scheduler 发送心跳，但发送过程中 Region 可能被
Split 操作同时修改。一开始我直接引用 p.Region()，后来意识到
这会导致 Scheduler 收到脏数据。

【方案对比】
我分析了两种方案：
1. 用锁：但锁只能保护读取瞬间，channel 发送是异步的，
   持有锁等待发送完成又可能阻塞其他操作，甚至死锁
2. 深拷贝：用 proto.Marshal + Unmarshal 创建独立副本

【最终选择】
我选择了深拷贝，原因是：
- 无阻塞、无死锁风险
- 网络传输本就需要序列化，这是顺带的
- 开销确定（~10μs），心跳 1 秒 1 次，完全可以接受

【收获】
这个设计让我理解了 ownership transfer 的重要性：
跨线程/协程传输的数据必须独立拥有，不能共享引用。
```

### Follow-up 问题准备

| 可能的问题 | 回答要点 |
|------------|----------|
| 为什么 `sync.Map` 不行？ | `sync.Map` 保证 Load/Store 原子性，但不能保护 Load 后的引用 |
| 深拷贝的性能开销？ | ~10μs/次，心跳 1 次/秒，总开销 0.001% |
| 还有其他并发保护措施吗？ | 有，storeMeta 用 RWMutex、router 用消息驱动模型 |
| 什么时候用锁，什么时候用拷贝？ | 小数据高频用锁，大数据低频用拷贝，跨协程传输用拷贝 |

---

## 2. Follower Apply 设计：突破标准 Raft 的 Leader-only 模式

### 问题背景

标准 Raft 实现中，只有 Leader 节点会将 committed entry Apply 到状态机，Follower 仅复制日志不执行。这导致：
- Follower 无法处理读请求，所有读流量集中到 Leader
- Leader 宕机后，新 Leader 需要从 Apply 位置重新执行，切换慢

### 解决方案

TinyKV 采用**所有节点同步 Apply**的设计：
- Leader 和 Follower 都将 committed entry Apply 到本地 Badger
- 所有节点的 KV 数据状态完全一致

### 收益

1. **快速故障切换**：Follower 随时可以成为 Leader 立即提供服务
2. **本地读取优化**：Follower 可以处理只读请求（如 Get），分散读负载
3. **状态一致性**：所有节点数据完全相同，简化调试和问题排查

### 面试回答模板

```
面试官：说说你项目的亮点设计？

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

## 3. CompactLog 防御性设计：防止日志 GC 回退

### 问题背景

CompactLog 用于截断已提交的旧 Raft 日志。在实现中发现潜在的并发问题：如果多个 CompactLog 请求乱序到达，可能导致 `LastCompactedIdx` 回退，GC 任务的范围不正确。

### 解决方案

添加防御性检查：
```go
if adminReq.CompactLog.CompactIndex > d.peerStorage.applyState.TruncatedState.Index {
    // 只有新的 CompactIndex 更大才执行
    truncatedState.Index = adminReq.CompactLog.CompactIndex
    d.ScheduleCompactLog(adminReq.CompactLog.CompactIndex)
}
```

### 收益

- 防止日志截断位置回退
- 确保 GC 任务的范围正确性
- 即使 CompactLog 消息乱序到达也能正确处理

### 面试回答模板

```
面试官：说说你如何保证系统可靠性的？

我：在 CompactLog 实现中，我发现了一个潜在的并发问题并添加了防御性检查。

【问题】
多个 CompactLog 请求可能乱序到达，导致 LastCompactedIdx 回退，
GC 可能删除错误的日志范围。

【解决】
添加了 CompactIndex > TruncatedState.Index 的检查，
只有更新的 CompactIndex 才会执行，防止回退。

【收获】
这让我理解了防御性编程的重要性：即使理论上不会发生，
也要在代码层面防止异常状态。
```

---

## 4. Region Split 实现：完整的动态分裂机制

### 问题背景

单 Region 过大会导致数据热点和负载不均衡。需要实现自动分裂机制，将大 Region 拆分成多个小 Region。

### 核心实现

1. **Split 验证**：4 层验证确保请求合法
   - RegionId 匹配检查
   - Epoch 版本检查
   - SplitKey 范围检查
   - Peer 数量一致性检查

2. **数据范围更新**：原子性更新 storeMeta
   - B-Tree 管理 Region 范围（支持 O(log N) 查找）
   - Map 管理 RegionId 映射

3. **新 Peer 创建与启动**：
   - 继承旧 Peer 的 StoreId（无需数据搬迁）
   - 通过 router 注册并发送 MsgTypeStart 启动

4. **分裂原则**：
   - 只分裂数据范围，不改变副本配置
   - 新旧 Region 副本数必须相同

### 设计亮点

| 设计点 | 说明 |
|--------|------|
| StoreId 继承 | 新 Peer 继承旧 Peer 的 StoreId，Split 瞬间完成 |
| 两阶段策略 | 先原地分裂，后 Scheduler 搬迁实现负载均衡 |
| 消息驱动启动 | 统一通过 MsgTypeStart 启动，并发安全 |

### 面试回答模板

```
面试官：说说 Region Split 的实现？

我：Region Split 是将大 Region 分裂成两个小 Region 的核心机制。

【触发条件】
Scheduler 定期检查 Region 大小，超过阈值（如 100MB）触发分裂。

【核心流程】
1. 4 层验证确保请求合法（RegionId、Epoch、SplitKey、Peer 数量）
2. 更新 storeMeta（B-Tree + Map，原子性）
3. 创建新 Peer（继承 StoreId，无需数据搬迁）
4. 注册并启动新 Peer（消息驱动模型）

【设计亮点】
- StoreId 继承：Split 瞬间完成，无需数据搬迁
- 两阶段策略：先分裂，后 Scheduler 平衡负载
- 消息驱动：统一通过 MsgTypeStart 启动，并发安全
```

---

## 5. Region 路由与 Key 查找：B-Tree 驱动的高效查询

### 核心机制

```
Key ∈ Region ⟺ StartKey ≤ Key < EndKey

比较原理：字节字典序（Lexicographical Order）
- 逐字节比较 ASCII 值
- "" < "a" < "abc" < "m" < "mysql" < "z"
```

### 数据结构

```go
type storeMeta struct {
    sync.RWMutex
    regionRanges *btree.BTree      // 按 StartKey 排序，范围查询
    regions      map[uint64]*Region // RegionID 映射，精确查询
}
```

### 查找流程

```
Client: Get("abc")
    ↓
1. Scheduler/Client 缓存：二分查找 O(log N)
   "" → Region1, "m" → Region2, "z" → Region3
    ↓
2. Store 接收：B-Tree 范围查询 O(log N)
   "abc" >= "" && "abc" < "m" → Region1
    ↓
3. 转发到对应 Peer 处理
```

### 面试回答模板

```
面试官：如何快速定位 Key 在哪个 Region？

我：TinyKV 使用基于字典序的 B-Tree 路由表。

【比较原理】
字节字典序：逐字节比较 ASCII 值
"a"(97) < "m"(109)，所以 "abc" < "m"

【数据结构】
- B-Tree：按 StartKey 排序，支持 O(log N) 范围查询
- Map：RegionID → Region，支持精确查找

【查找流程】
1. Client 缓存二分查找
2. Store B-Tree 范围查询
3. 转发到对应 Peer

【收益】
即使有百万级 Region，查找也是 O(log N) 亚毫秒级
```

---

## 6. Region Split 的核心动机：解决热点问题

### 问题

单 Region 过大导致：
- 所有请求集中到一个 Leader，形成热点瓶颈
- 无法利用多 Store 的计算能力
- 加机器也无法扩容（Region 还是在一个 Store 上）

### 解决方案

Split 将大 Region 拆分成多个小 Region，Scheduler 将不同 Region 的 Leader 分散到不同 Store。

### 效果

```
Split 前：
Region1(L) → Store1  ← 所有请求打到这里

Split 后：
Region1(L) → Store1
Region2(L) → Store3  ← 负载分散！
```

### 面试回答模板

```
面试官：为什么需要 Region Split？

我：核心是解决数据热点和负载均衡问题。

【问题】
单 Region 过大时，所有请求集中到一个 Leader，
形成单点瓶颈，加机器也无法扩容。

【解决】
Split 将大 Region 拆成多个小 Region，
Scheduler 将 Leader 分散到不同 Store。

【本质】
这是 Auto-Sharding 的基石，让系统能够随数据量
增长而线性扩展，无需人工干预分片。
```

---

## 总结：面试策略建议

### 优先级排序

| 亮点 | 优先级 | 适用场景 |
|------|--------|----------|
| Follower Apply | ⭐⭐⭐⭐⭐ | 主要亮点，展示架构思考 |
| Region Split 整体 | ⭐⭐⭐⭐⭐ | 主要亮点，展示系统能力 |
| 深拷贝 vs 锁 | ⭐⭐⭐⭐ | 并发问题追问时的例证 |
| CompactLog 防御性 | ⭐⭐⭐ | 可靠性/错误处理追问 |
| Region 路由 | ⭐⭐⭐ | 数据结构/算法追问 |
| Split 动机 | ⭐⭐⭐⭐ | 分布式系统基础理解 |

### 组合拳建议

```
面试官：说说你项目中印象最深的技术问题？

推荐组合：
1. Follower Apply（架构设计亮点）
2. Region Split（完整功能实现）
3. 深拷贝 vs 锁（并发细节思考）

这样既有宏观架构，又有微观细节，展示全面能力。
```

### 通用回答框架（STAR 法则）

```
Situation（情境）：项目背景、问题场景
Task（任务）：你的职责、目标
Action（行动）：你的方案、实现、权衡
Result（结果）：效果、收益、收获
```

---

## 附：TinyKV 技术栈关键词

- **分布式系统**：Raft 共识、Multi-Raft、Region Split、Leader 选举
- **并发编程**：RWMutex、sync.Map、Channel、消息驱动模型
- **存储引擎**：Badger KV、LSM-Tree、WriteBatch、Column Family
- **协议**：Protocol Buffers、gRPC、Raft Protocol
- **设计模式**：Ready Pattern、Callback Pattern、Router Pattern
- **工程实践**：防御性编程、Ownership Transfer、深拷贝 vs 锁
