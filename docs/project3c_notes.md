# Project 3C - Scheduler Balance 个人笔记

## 一、3C 要解决什么问题？

3A 解决了 Raft 层的 Leader Transfer 和 ConfChange，3B 解决了 raftstore 层的 Region Split 和 ConfChange 联动。3C 要解决的问题是：**集群运行一段时间后，Region 在各 Store 上的分布不均匀**。

举个例子：3 个 Store，每个 Region 3 副本。随着 Split 和写入，Store1 可能堆积了 100 个 Region，Store2 只有 30 个，Store3 只有 20 个。这种不均会导致：
- Store1 成为热点，负载远超其他节点
- 存储空间使用不均，Store1 可能先满
- 请求延迟不均匀

3C 实现两个核心功能：
1. **processRegionHeartbeat** — Scheduler 接收并处理 Region 心跳，维护集群元数据
2. **balanceRegionScheduler** — 基于 Region 大小的负载均衡调度器

---

## 二、整体架构：Scheduler 如何驱动均衡？

在深入两个实现之前，先理解 Scheduler 的整体工作模型：

```
┌─────────────────────────────────────────────────────────────────┐
│  TiKV Store (每个 Region 的 Leader)                              │
│   定时发送 RegionHeartbeatRequest                                │
│   包含: Region元数据, Leader信息, PendingPeers, ApproximateSize  │
└───────────────────────────┬─────────────────────────────────────┘
                            │ gRPC Stream
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  Scheduler Server                                                │
│                                                                  │
│  ① grpc_service.RegionHeartbeat()                               │
│     接收心跳流，提取 RegionInfo                                   │
│              │                                                   │
│              ▼                                                   │
│  ② cluster.processRegionHeartbeat(region)   ← 3C 要实现         │
│     校验 Epoch，更新 RegionTree 和 Store 状态                     │
│              │                                                   │
│              ▼                                                   │
│  ③ coordinator.opController.Dispatch(region)                     │
│     检查该 Region 是否有待执行的 Operator                         │
│     如果有，通过心跳响应下发调度命令                                │
│                                                                  │
│  ┌─────────────────────────────────────────┐                    │
│  │  coordinator 后台循环                     │                    │
│  │  ④ balanceRegionScheduler.Schedule()     │ ← 3C 要实现       │
│  │     选出源Store → 选Region → 选目标Store  │                    │
│  │     生成 MovePeer Operator               │                    │
│  │              │                            │                    │
│  │              ▼                            │                    │
│  │  ⑤ opController.AddOperator(op)          │                    │
│  │     将 Operator 加入待执行队列             │                    │
│  └─────────────────────────────────────────┘                    │
└───────────────────────────┬─────────────────────────────────────┘
                           │ 心跳响应中携带调度命令
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  TiKV Store 执行调度命令                                          │
│   TransferLeader / AddPeer / RemovePeer                         │
└─────────────────────────────────────────────────────────────────┘
```

**关键洞察**：Scheduler 不直接操作 TiKV Store，而是通过**心跳响应**下发调度命令。Store 收到后自行执行 Raft 层的 ConfChange 等操作。这是典型的"拉模型"——Store 主动汇报，Scheduler 被动响应。

---

## 三、processRegionHeartbeat 详解

**位置**: `scheduler/server/cluster.go:280`

### 3.1 Region 心跳是什么？

每个 Region 的 Leader 会定时向 Scheduler 发送心跳，包含：
- **Region 元数据**: ID, StartKey, EndKey, Peers, RegionEpoch
- **Leader 信息**: 哪个 Peer 是 Leader
- **PendingPeers**: 日志落后的 Peer
- **ApproximateSize**: Region 估算大小

Scheduler 通过心跳感知集群的实时状态。

### 3.2 为什么要做 Epoch 校验？

心跳是异步的，可能乱序到达。比如：
- Split 后旧 Region 的心跳晚于新 Region 的心跳到达
- ConfChange 后旧配置的心跳还没处理完

如果不校验，就会用过时信息覆盖最新信息，导致 Scheduler 的世界观"倒退"。

### 3.3 完整流程

```
processRegionHeartbeat(region)
    │
    ├─ ① Epoch 为空？ → 返回错误（防御性检查）
    │
    ├─ ② 查找同 ID 的旧 Region
    │      │
    │      ├─ 旧 Region 存在 → 比较 Epoch
    │      │   │
    │      │   ├─ 新 ConfVer < 旧 ConfVer → 过时心跳，拒绝
    │      │   ├─ 新 Version < 旧 Version → 过时心跳，拒绝
    │      │   └─ 否则 → 通过校验
    │      │
    │      └─ 旧 Region 不存在（新 Region，可能是 Split 产生的）
    │         │
    │         ├─ ③ 扫描 KeyRange 重叠的所有 Region
    │         │   │
    │         │   └─ 对每个重叠 Region 比较 Epoch
    │         │      ├─ 新 Epoch 任一维度 < 旧 → 过时，拒绝
    │         │      └─ 否则 → 通过校验
    │
    ├─ ④ 校验通过 → putRegion(region) 更新 RegionTree
    │
    └─ ⑤ 更新所有相关 Store 的状态统计
         for storeId := range region.GetStoreIds() {
             updateStoreStatusLocked(storeId)
         }
```

### 3.4 两种校验场景详解

**场景 1：同 ID 已存在 — 比较直接**

```
Region ID=1, ConfVer=3, Version=2  (Scheduler 已有)
Region ID=1, ConfVer=2, Version=3  (新到的心跳)

结果：ConfVer(2) < 旧 ConfVer(3) → 拒绝
原因：虽然 Version 更高了，但 ConfVer 倒退了
     说明这个心跳是基于旧配置发出的，不能信任
```

**场景 2：同 ID 不存在 — 检查 KeyRange 重叠**

这是 Split 的典型场景。Split 后产生新 Region，新 Region 的 ID 与旧 Region 不同：

```
Split 前: Region 1, [a, z), Version=1
Split 后: Region 1, [a, m), Version=2  ← 旧 Region 缩小了范围
          Region 2, [m, z), Version=2  ← 新 Region

如果 Region 2 的心跳先到达：
  - Scheduler 中没有 Region 2 → 新 Region
  - ScanRegions([m, z)) 找到 Region 1（范围重叠）
  - Region 2.Version(2) >= Region 1.Version(1) → 通过
  - putRegion(Region 2) → RegionTree 中新增 Region 2

如果后续收到旧的 Region 1 心跳（Version=1）：
  - Scheduler 中已有 Region 1 → 同 ID 存在
  - 新 Version(1) < 旧 Version(2) → 拒绝
```

### 3.5 为什么需要两阶段校验？

| 校验方式 | 覆盖场景 | 不足 |
|---------|---------|------|
| 同 ID 比较 | ConfChange、同一 Region 的 Split | 无法检测 Split 产生的新 Region |
| KeyRange 重叠 | Split 产生的新 Region | 新 Region 可能覆盖多个旧 Region |

两种校验互补：同 ID 存在用直接比较，不存在时用 KeyRange 重叠比较。**核心原则：任何一个维度的 Epoch 倒退，都意味着过时信息。**

### 3.6 putRegion 和 updateStoreStatusLocked

```go
c.putRegion(region)  // 更新 RegionTree（B-Tree 索引，按 KeyRange 组织）
for i := range region.GetStoreIds() {
    c.updateStoreStatusLocked(i)  // 更新每个 Store 的统计信息
}
```

`updateStoreStatusLocked` 刷新的是 Store 维度的聚合数据：
- `leaderCount` — 该 Store 上有多少 Leader
- `regionCount` — 该 Store 上有多少 Region
- `pendingPeerCount` — 该 Store 上有多少 Pending Peer
- `leaderRegionSize` / `regionSize` — 该 Store 上 Leader/总 Region 的大小

这些数据正是 balanceRegionScheduler 做决策的依据。

---

## 四、balanceRegionScheduler 详解

**位置**: `scheduler/server/schedulers/balance_region.go`

### 4.1 核心思路

**找最重的 Store，搬一个 Region 到最轻的 Store**。就像搬砖——从砖多的地方搬一块到砖少的地方。

但不是简单粗暴地搬，需要满足多重约束。

### 4.2 完整流程

```
Schedule(cluster)
    │
    ├─ ① 筛选 SuitableStores
    │   条件: Store.IsUp() && DownTime < MaxStoreDownTime
    │   至少需要 2 个 Store
    │
    ├─ ② 按 RegionSize 从小到大排序
    │
    ├─ ③ 从最大的 Store 开始，尝试找一个可搬的 Region
    │   优先级: Pending > Follower > Leader
    │   │
    │   │  为什么优先级是 Pending > Follower > Leader？
    │   │  - Pending: 正在迁移但未完成，搬走代价最小
    │   │  - Follower: 不是 Leader，搬走不影响读写服务
    │   │  - Leader: 搬走需要先 TransferLeader，代价最大
    │   │
    │   └─ 找到 fromStore + region → break
    │
    ├─ ④ 安全检查: Region 副本数 < MaxReplicas → 放弃
    │   原因: 副本不足时，补副本比均衡更重要
    │
    ├─ ⑤ 从最小的 Store 开始，找目标 Store
    │   条件: 目标 Store 不在该 Region 的副本列表中
    │   （不能在已有副本的 Store 上再创建副本）
    │
    ├─ ⑥ 收益检查: fromSize - toSize < regionSize → 放弃
    │   搬完后差距反而变小了？那搬了也没意义
    │
    └─ ⑦ 创建 MovePeer Operator 并返回
        AllocPeer(toStoreID) → 创建新 Peer
        CreateMovePeerOperator() → 生成操作步骤
```

### 4.3 举例说明

假设 `MaxReplicas=3`，集群状态：

```
Store 1: 150MB (最大)
Store 2: 80MB
Store 3: 30MB  (最小)

排序后: [Store3(30), Store2(80), Store1(150)]
```

**Step ③ — 从 Store1 找 Region：**

```
Store1 上的 Region:
  - Region A (Leader):  20MB
  - Region B (Follower): 15MB
  - Region C (Pending):  10MB  ← 优先选这个

选中: fromStore=Store1, region=RegionC(10MB)
```

**Step ④ — 安全检查：**
```
RegionC 的副本在 Store1, Store2, Store3 → 3 个副本
3 >= MaxReplicas(3) → 通过
```

**Step ⑤ — 找目标 Store：**
```
Store3 的 RegionC 副本？已有 → 跳过
Store2 的 RegionC 副本？已有 → 跳过
没有其他 Store → toStore = nil → 返回 nil

等等！所有 Store 都已有副本，无法找到目标。
这说明 3 副本 Region 在 3 Store 集群中无法做均衡。
需要更多 Store 才能搬。
```

**扩容到 4 Store 后：**

```
Store 1: 150MB
Store 2: 80MB
Store 3: 30MB
Store 4: 20MB  (新加入)

排序后: [Store4(20), Store3(30), Store2(80), Store1(150)]

Step ③: fromStore=Store1, region=RegionC(10MB)
Step ④: 3 副本 >= 3 → 通过
Step ⑤: Store4 不在 RegionC 的副本列表 → toStore=Store4
Step ⑥: 150 - 20 = 130 > 10 → 收益足够，通过

结果: 创建 MovePeer(Store1 → Store4, RegionC)
```

### 4.4 关键设计决策

**1. 为什么从最大 Store 开始找 Region？**

直觉上是"搬最重的Store"，但更深的原因是**稳定性**。如果从小 Store 开始，可能把 Region 搬到更大的 Store，反而加剧不均衡。从大 Store 往小 Store 搬，保证每次调度都让集群更均衡。

**2. 为什么副本不足时放弃均衡？**

```go
if len(storeIds) < cluster.GetMaxReplicas() {
    return nil  // 放弃
}
```

假设 3 副本的 Region 只有 2 个副本存活（一个 Store 挂了）。此时做均衡意味着从一个只剩 2 副本的 Region 中搬走一个副本，变成 1 副本——风险极大。**数据安全 > 负载均衡**，这是分布式系统的铁律。

**3. 为什么收益检查用 `region.GetApproximateSize()` 而不是固定阈值？**

```go
if fromStore.GetRegionSize()-toStore.GetRegionSize() < region.GetApproximateSize() {
    return nil
}
```

如果搬完这个 Region 后，fromStore 的 RegionSize 反而小于 toStore（即"矫枉过正"），那不如不搬。比较的是**差值 vs Region大小**，确保搬完后 from 仍然 >= to，至少不会恶化。

### 4.5 MovePeer Operator 的执行

`CreateMovePeerOperator` 生成的 Operator 包含以下步骤（按顺序执行）：

```
Operator: MovePeer(RegionC, Store1 → Store4)

Step 1: AddPeer(Store4, NewPeerID)     → 通知 Store4 加入 RegionC
Step 2: TransferLeader (if needed)     → 如果 Store1 是 Leader，先转移
Step 3: RemovePeer(Store1, OldPeerID)  → 通知 Store1 退出 RegionC
```

这些步骤通过**心跳响应**下发给对应的 TiKV Store：

```
Scheduler 发送 RegionHeartbeatResponse:
  - 给 Store4: ChangePeer(AddNode, NewPeer)
  - 给 Store1: ChangePeer(RemoveNode, OldPeer) 或 TransferLeader

Store 收到后执行 Raft ConfChange → Region 副本实际迁移
```

---

## 五、两个实现的协同关系

```
processRegionHeartbeat          balanceRegionScheduler
        │                               │
   维护集群状态                      基于集群状态做决策
        │                               │
   更新 RegionTree                   读取 Store.RegionSize
   更新 Store 统计                   读取 Region.GetStoreIds()
        │                               │
        └────────── 互为因果 ───────────┘
                   ↗                ↘
   心跳处理 → 更新状态 → 调度决策 → 下发命令
      ↑                                  │
      └──────────── Store 执行后上报 ──────┘
```

**processRegionHeartbeat 是"眼睛"**：感知集群变化，维护准确的状态
**balanceRegionScheduler 是"大脑"**：基于状态做出均衡决策

没有准确的状态，调度器会做出错误决策。没有调度器，状态维护就失去了意义。两者缺一不可。

---

## 六、核心数据结构速查

### RegionEpoch

| 字段 | 何时自增 | 追踪什么 |
|------|---------|---------|
| `conf_ver` | AddPeer / RemovePeer | 副本配置变化 |
| `version` | Split / Merge | 数据范围变化 |

### RegionInfo

| 字段 | 含义 | 调度用途 |
|------|------|---------|
| `meta` | Region 元数据（ID, KeyRange, Peers, Epoch） | 校验、路由 |
| `leader` | 当前 Leader Peer | 区分 Leader/Follower Region |
| `pendingPeers` | 滞后 Peer | 优先搬 Pending Region |
| `approximateSize` | 估算大小（MB） | 收益检查、Split 判断 |

### StoreInfo 关键指标

| 方法 | 含义 | 调度用途 |
|------|------|---------|
| `GetRegionSize()` | Store 上所有 Region 的总大小 | 排序、选源/目标 Store |
| `IsUp()` | Store 是否在线 | 筛选可用 Store |
| `DownTime()` | 停机时长 | 排除宕机 Store |
| `GetID()` | Store ID | 生成调度命令 |

---

## 七、踩坑与设计亮点

### 7.1 Split 产生新 Region 时的 KeyRange 重叠检查

如果只做同 ID 的 Epoch 比较，Split 产生的新 Region（新 ID）会直接通过校验，可能覆盖旧 Region 的数据。用 `ScanRegions(StartKey, EndKey)` 检查 KeyRange 重叠，确保新 Region 的 Epoch 不低于任何被它替代的旧 Region。

### 7.2 心跳驱动模型

Scheduler 不主动连接 Store，完全依赖心跳被动感知。好处是简单、无需维护连接池；坏处是感知延迟取决于心跳间隔。这也是为什么心跳频率不能太低。

### 7.3 均衡目标不是 Region 数量，而是磁盘空间

直觉上容易以为 balance-region 是按 **Region 数量** 做均衡——哪个 Store 上 Region 多就往少的搬。但实际上排序和收益检查用的全是 `GetRegionSize()`（磁盘空间），不是 `GetStoreRegionCount()`（Region 数量）。

**为什么？** Region 大小差异极大。一个 Store 可能有 100 个空 Region（共 100MB），另一个只有 10 个大 Region（共 1GB）。按数量均衡会把空 Region 搬来搬去，磁盘使用依然严重不均。按空间均衡才能真正解决"磁盘快满了"这个运维痛点。

这也是 `storeSlice.Less()` 用 `GetRegionSize()` 而非 Region count 的原因：

```go
func (a storeSlice) Less(i, j int) bool {
    return a[i].GetRegionSize() < a[j].GetRegionSize()  // 按磁盘空间排序
}
```

### 7.4 Pending > Follower > Leader 优先级的原因

从一个 Store 选 Region 搬出去时，优先级是：Pending > Follower > Leader。

**搬 Follower 副本 vs 搬 Leader 副本，代价完全不同：**

```
搬 Follower 副本：
  Store A (Follower) → Store D
  客户端请求走 Leader → 不受影响 ✓
  只需同步数据，没有选举 ✓

搬 Leader 副本：
  Store A (Leader) → Store D
  1. 先要触发选举，选出新 Leader    ← 期间客户端请求会卡住
  2. 原 Leader 变成 Follower
  3. 再搬这个 Follower
  代价大，能不搬就不搬 ✗
```

**容易混淆的点：** 选的是"Region"，不是"节点"。搬的是这个 Region 在该 Store 上的一个 Peer（副本）。一个 Store 上可能同时有作为 Leader 的 Region 和作为 Follower 的 Region，优先搬 Follower 那些就对了。

### 7.5 两个独立的均衡调度器：数据 vs Leader

TinyKV 有两个独立的均衡调度器，解决不同维度的不均衡：

| 调度器 | 目标 | 操作方式 | 文件 |
|--------|------|---------|------|
| balanceRegionScheduler | 均衡数据大小 | MovePeer（搬副本） | balance_region.go |
| balanceLeaderScheduler | 均衡 Leader 数量 | TransferLeader（转移领导权） | balance_leader.go |

**为什么只均衡数据量不够？**

```
Store A: 10 个 Region，其中 8 个是 Leader  ← 客户端请求全压在这
Store B: 10 个 Region，其中 2 个是 Leader  ← 很闲
Store C: 10 个 Region，其中 0 个是 Leader  ← 更闲

数据量是均衡的（每个 Store 10 个 Region）
但请求负载完全不均衡！
```

客户端的读写请求都走 Leader，所以 **Leader 分布 = 请求负载分布**。只均衡数据量不够，还要均衡 Leader。

**两种操作方式的代价对比：**

```
搬 Region (MovePeer): 搬数据，代价大
  Store A → Store D: 加 Peer、同步数据、删旧 Peer

转移 Leader (TransferLeader): 不搬数据，代价小
  Store A (Leader) → Store B (Follower):
    Store A 发起 TransferLeader
    Store B 当选新 Leader
    Store A 变成 Follower
    数据不动！
```

**balanceLeaderScheduler 的流程与 balanceRegionScheduler 类似：**

1. 筛选在线 Store
2. 按 Leader 数量排序
3. 从 Leader 最多的 Store 找一个 Region
4. 找到该 Region 的 Follower 所在 Store 作为目标
5. 差值太小就不转
6. 生成 TransferLeader 操作返回

**核心思路一样：从多的搬到少的。差别只是操作方式——数据不均衡用 MovePeer（搬副本），Leader 不均衡用 TransferLeader（转移领导权）。**

### 7.6 收益检查防止"无效搬迁"

没有收益检查的话，可能出现"震荡"：把 Region 从 A 搬到 B，下一轮又从 B 搬回 A。收益检查保证每次搬迁都让集群更均衡，不会恶化。
