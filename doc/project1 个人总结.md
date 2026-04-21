# Project1 个人总结

## 一、技术文档：基于 BadgerDB 的单机 KV 存储实现

### 1. 提交信息

**Commit**: `ab7dc1f8893ee5c4609d6c260d521f346d93624b`

**修改文件**:
- `kv/server/raw_api.go` (75 行新增)
- `kv/storage/standalone_storage/standalone_storage.go` (70 行新增)

### 2. 架构概述

```
┌─────────────────────────────────────────────────────────┐
│                      Client (TinySQL)                    │
└────────────────────┬────────────────────────────────────┘
                     │ gRPC (Raw API)
                     ▼
┌─────────────────────────────────────────────────────────┐
│                        Server                            │
│  ┌──────────────────────────────────────────────────┐   │
│  │  RawGet / RawPut / RawDelete / RawScan           │   │
│  └──────────────────────┬───────────────────────────┘   │
│                         │                                │
│                         ▼                                │
│  ┌──────────────────────────────────────────────────┐   │
│  │                   Storage                        │   │
│  │  ┌────────────────────────────────────────────┐  │   │
│  │  │          StandAloneStorage                 │  │   │
│  │  │  ┌─────────────┐    ┌──────────────────┐   │  │   │
│  │  │  │  Reader     │    │  Writer          │   │  │   │
│  │  │  │  (Txn)      │    │  (Modifies)      │   │  │   │
│  │  │  └─────────────┘    └──────────────────┘   │  │   │
│  │  └────────────────────────────────────────────┘  │   │
│  └──────────────────────────────────────────────────┘   │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│                    BadgerDB (LSM-Tree)                   │
│  ┌─────────────┐  ┌─────────────┐                        │
│  │   KV DB     │  │  Raft DB    │ (预留给 Raft)          │
│  │ (Default/W) │  │  (Lock/...) │                        │
│  └─────────────┘  └─────────────┘                        │
└─────────────────────────────────────────────────────────┘
```

### 3. 核心数据结构

#### 3.1 StandAloneStorage

```go
type StandAloneStorage struct {
    engines *engine_util.Engines  // 管理 KV 和 Raft 两个 Badger 实例
    config  *config.Config
}
```

**设计要点**:
- 使用 `engine_util.Engines` 统一管理多个 Column Family (CF)
- KV 数据存储在 `kv` 子目录，Raft 数据存储在 `raft` 子目录
- 支持多 CF：`Default`(数据)、`Write`(事务)、`Lock`(锁)

#### 3.2 Column Family 设计

| CF | 用途 | 说明 |
|----|------|------|
| `default` | 实际数据 | 存储真正的 key-value 数据 |
| `lock` | 锁信息 | 存储事务锁信息 |
| `write` | 提交信息 | 存储事务的 commit 信息 |

### 4. 接口实现详解

#### 4.1 RawGet - 点查接口

**功能**: 根据 CF 和 Key 获取对应的 Value

**实现流程**:
```
1. 通过 storage.Reader() 获取只读事务
2. 调用 reader.GetCF(cf, key) 读取数据
3. 处理 KeyNotFound 情况 (返回 nil, NotFound=true)
4. defer reader.Close() 确保事务正确关闭
```

**关键代码**:
```go
func (server *Server) RawGet(ctx context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
    reader, err := server.storage.Reader(req.Context)
    if err != nil {
        return nil, err
    }
    defer reader.Close()
    
    val, err := reader.GetCF(req.Cf, req.Key)
    if err != nil {
        return nil, err
    }
    
    resp := &kvrpcpb.RawGetResponse{
        Value:    val,
        NotFound: false,
    }
    if val == nil {
        resp.NotFound = true
    }
    return resp, nil
}
```

**设计思考**:
- **为什么需要 Reader?** BadgerDB 使用 MVCC，读取需要在一致性的快照中进行
- **为什么需要 Close?** 释放事务持有的资源，避免内存泄漏
- **NotFound 语义**: 返回 `nil` 值 + `NotFound=true`，区分"值为空"和"键不存在"

#### 4.2 RawPut - 写入接口

**功能**: 将 key-value 对写入存储

**实现流程**:
```
1. 构造 storage.Put 对象
2. 封装为 storage.Modify 批量操作
3. 调用 storage.Write() 执行写入
```

**关键代码**:
```go
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
    put := storage.Put{
        Key:   req.Key,
        Value: req.Value,
        Cf:    req.Cf,
    }
    batch := storage.Modify{Data: put}
    err := server.storage.Write(req.Context, []storage.Modify{batch})
    if err != nil {
        return nil, err
    }
    return &kvrpcpb.RawPutResponse{}, nil
}
```

**设计思考**:
- **批量接口设计**: 使用 `[]storage.Modify` 支持批量写入，为后续事务操作铺垫
- **零返回值**: Put 操作只需返回成功/失败，不需要额外信息

#### 4.3 RawDelete - 删除接口

**功能**: 从存储中删除指定的 key

**关键代码**:
```go
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
    delete := storage.Delete{
        Key: req.Key,
        Cf:  req.Cf,
    }
    batch := storage.Modify{Data: delete}
    err := server.storage.Write(req.Context, []storage.Modify{batch})
    if err != nil {
        return nil, err
    }
    return &kvrpcpb.RawDeleteResponse{}, nil
}
```

#### 4.4 RawScan - 范围扫描接口

**功能**: 从 StartKey 开始扫描，最多返回 Limit 个 KV 对

**实现流程**:
```
1. 获取 Reader (只读事务)
2. 获取 Column Family 迭代器
3. Seek 到 StartKey
4. 迭代直到达到 Limit 或迭代器无效
5. 收集结果并返回
```

**关键代码**:
```go
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
    reader, err := server.storage.Reader(req.Context)
    if err != nil {
        return nil, err
    }
    defer reader.Close()
    
    iter := reader.IterCF(req.Cf)
    defer iter.Close()

    var Kvs []*kvrpcpb.KvPair
    limit := req.Limit

    for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
        item := iter.Item()
        key := item.KeyCopy(nil)
        val, _ := item.ValueCopy(nil)

        Kvs = append(Kvs, &kvrpcpb.KvPair{
            Key:   key,
            Value: val,
        })
        limit--
        if limit == 0 {
            break
        }
    }
    return &kvrpcpb.RawScanResponse{Kvs: Kvs}, nil
}
```

**设计思考**:
- **迭代器模式**: 使用迭代器避免一次性加载大量数据到内存
- **KeyCopy/ValueCopy**: Badger 的 Item 数据在事务结束后失效，需要拷贝
- **Limit 限制**: 防止全表扫描，保护系统资源

### 5. 存储层实现

#### 5.1 初始化

```go
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
    dbPath := conf.DBPath
    kvPath := path.Join(dbPath, "kv")
    raftPath := path.Join(dbPath, "raft")

    kvDB := engine_util.CreateDB(kvPath, false)
    raftDB := engine_util.CreateDB(raftPath, true)
    
    return &StandAloneStorage{
        engines: engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath),
        config:  conf,
    }
}
```

#### 5.2 写入实现

```go
func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
    var err error
    for _, m := range batch {
        key, val, cf := m.Key(), m.Value(), m.Cf()
        if _, ok := m.Data.(storage.Put); ok {
            err = engine_util.PutCF(s.engines.Kv, cf, key, val)
        } else {
            err = engine_util.DeleteCF(s.engines.Kv, cf, key)
        }
        if err != nil {
            return err
        }
    }
    return nil
}
```

#### 5.3 读取实现

```go
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
    kvTxn := s.engines.Kv.NewTransaction(false) // false = 只读事务
    return NewStandAloneReader(kvTxn), nil
}

type StandAloneReader struct {
    kvTxn *badger.Txn
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
    val, err := engine_util.GetCFFromTxn(r.kvTxn, cf, key)
    if err == badger.ErrKeyNotFound {
        return nil, nil
    }
    return val, err
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
    return engine_util.NewCFIterator(cf, r.kvTxn)
}

func (r *StandAloneReader) Close() {
    r.kvTxn.Discard()
}
```

---

## 二、实验心得与思考

### 1. 遇到的难点

#### 难点一：Windows 文件锁问题

**现象**:
```
2026/04/15 17:03:45 log.go:125: [fatal] [While removing table 1: remove \tmp\badger\kv\00000001.sst: 
The process cannot access the file because it is being used by another process.]
```

**实际遇到的测试失败**:
```bash
go test -v --count=1 --parallel=1 -p=1 ./kv/server -run 1
=== RUN   TestRawGet1
--- PASS: TestRawGet1 (0.89s)
=== RUN   TestRawGetNotFound1
[fatal] [While removing table 1: remove \tmp\badger\kv\00000001.sst: 
The process cannot access the file because it is being used by another process.]
FAIL
```

**规律**: 第一个测试通过后，第二个测试清理时出现文件锁问题。

**根本原因分析**:
1. BadgerDB 使用 **内存映射文件 (mmap)** 存储 SSTable
2. Windows 的文件锁机制与 Unix 不同：
   - Unix:  advisory lock (建议锁)，进程退出自动释放
   - Windows: mandatory lock (强制锁)，文件句柄未完全释放前无法删除
3. Go 的垃圾回收时机不确定，可能导致 `defer reader.Close()` 后 txn 对象仍未被 GC

**解决方案**:
```go
// 方案 1: 确保所有资源正确关闭
reader, err := server.storage.Reader(req.Context)
if err != nil { return nil, err }
defer reader.Close()  // 必须！

iter := reader.IterCF(req.Cf)
defer iter.Close()  // 必须！

// 方案 2: 测试前清理旧数据
func cleanUpTestData(conf *config.Config) error {
    return os.RemoveAll(conf.DBPath)
}
```

**深度思考**:
这个问题暴露了**跨平台开发的陷阱**。LSM-Tree 存储引擎在 Linux 上运行良好，但在 Windows 上遇到文件锁问题，这说明了:
1. 数据库开发必须在目标平台上充分测试
2. 文件系统抽象层的重要性 (如 RocksDB 的 `Env` 抽象)
3. 资源管理必须严格遵循 RAII 原则

---

#### 难点二：事务与迭代器的生命周期管理

**问题**: 最初实现 RawScan 时，忘记关闭迭代器，导致内存泄漏

**错误示例**:
```go
// 错误代码
iter := reader.IterCF(req.Cf)
// 忘记 defer iter.Close()
for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
    // ...
}
```

**为什么需要 Close?**
- Badger 的迭代器持有事务的引用
- 迭代器内部维护了 LSM-Tree 多层级的游标
- 不关闭会导致：
  1. 内存泄漏 (迭代器缓冲区不释放)
  2. 文件句柄泄漏 (SSTable 文件无法关闭)
  3. 测试失败 (Windows 文件锁问题)

**正确模式**:
```go
reader, err := server.storage.Reader(ctx)
if err != nil { return err }
defer reader.Close()

iter := reader.IterCF(cf)
defer iter.Close()

// 使用 iter...
```

---

### 2. 技术选型思考

#### 2.1 为什么选择 BadgerDB?

| 特性 | BadgerDB | LevelDB | RocksDB |
|------|----------|---------|---------|
| 语言 | Go | C++ | C++ |
| 部署难度 | 低 (纯 Go) | 中 (CGO) | 中 (CGO) |
| 性能 | 中 | 高 | 最高 |
| 功能 | 完整 | 基础 | 最完整 |
| 适用场景 | 教学、中小型应用 | 通用 | 大规模生产 |

**选型理由**:
1. **纯 Go 实现**: 避免 CGO 的跨平台编译问题
2. **文档完善**: 适合学习和理解 LSM-Tree
3. **功能完整**: 支持事务、多 CF、TTL 等

**但 Badger 也有局限**:
- 性能不如 RocksDB (官方基准测试显示约 1/3)
- 社区活跃度较低 (Dgraph 公司维护)
- 生产环境案例较少

#### 2.2 Column Family 的设计哲学

**问题**: 为什么不把所有数据存在一个 namespace，而要设计多 CF?

**答案**:
1. **IO 隔离**: 不同 CF 的数据可以存储在不同的磁盘上
2. **压缩策略独立**: `default` CF 可以用快速压缩，`write` CF 可以用高压缩比
3. **迭代器隔离**: 扫描 `default` CF 不会读取 `lock` 和 `write` 的数据
4. **事务语义**: TiKV 使用 CF 分离数据的不同版本

**TiKV 的 CF 设计**:
```
default CF:  实际数据 (value 可能很大)
lock CF:     当前事务的锁信息 (小 KV)
write CF:    提交信息 (MVCC 版本元数据)
```

这种设计是**以空间换时间 + 以分离换隔离**的经典案例。

---

### 3. 架构层面的思考

#### 3.1 分层架构的优势

```
┌─────────────────┐
│     Raw API     │  ← gRPC 接口层 (协议无关)
├─────────────────┤
│    Storage      │  ← 存储抽象层 (接口)
├─────────────────┤
│ StandAloneStore │  ← 单机实现 (可替换为 Raft)
├─────────────────┤
│   BadgerDB      │  ← 嵌入数据库 (可替换为其他)
└─────────────────┘
```

**优势**:
1. **可测试性**: 可以用 mock Storage 测试 Raw API
2. **可扩展性**: 后续实现 RaftStorage 无需修改 Raw API
3. **可替换性**: 可以轻松替换底层存储引擎

#### 3.2 读写分离设计

```go
// 读路径：Reader -> Txn -> Snapshot
reader, _ := storage.Reader(ctx)
defer reader.Close()
val, _ := reader.GetCF(cf, key)

// 写路径：Modify -> Batch -> WAL -> MemTable
storage.Write(ctx, []Modify{...})
```

**设计精髓**:
- 读操作在**快照**上进行，不阻塞写
- 写操作批量提交，减少 WAL 刷盘次数
- 读写互不干扰，提升并发性能

---

## 三、面试官拷打环节

### Q1: RawGet 中，为什么 val == nil 时要设置 NotFound = true? 直接返回不行吗?

**A**: 这是**语义完整性**的设计。

在 Go 中，`nil` 值有两种含义:
1. Key 不存在
2. Value 本身就是 nil/空

通过 `NotFound` 字段可以明确区分这两种情况:

```go
// 场景 1: Key 不存在
resp = {Value: nil, NotFound: true}

// 场景 2: Value 为空数组 (Key 存在)
resp = {Value: []byte{}, NotFound: false}
```

如果不设置 `NotFound`，客户端无法区分这两种情况。这是 Protocol Buffer 设计中常见的**null object pattern**。

---

### Q2: RawScan 中，如果 Limit 非常大 (比如 100 万)，会有什么问题？如何优化？

**A**: 会有以下问题:

1. **内存爆炸**: 所有 KV 对加载到内存，可能导致 OOM
2. **响应时间长**: 客户端等待时间过长
3. **锁持有时间长**: 事务持有快照时间过长，影响并发

**优化方案**:

```go
// 方案 1: 分页扫描 (推荐)
func RawScan(ctx, req) {
    limit := min(req.Limit, 1000)  // 限制单次最多返回 1000 条
    // ...
    // 客户端通过 LastKey 继续扫描
}

// 方案 2: 流式返回 (gRPC streaming)
func RawScanStream(req, stream) {
    for iter.Valid() {
        stream.Send(kv)  // 逐个发送
        // 可以定期 pause，让出处理时间
    }
}
```

TiKV 实际采用**方案 1**，通过 `start_key` + `limit` 实现分页。

---

### Q3: 为什么 Write 操作要接收 `[]storage.Modify` 而不是单个操作？

**A**: 这是为**批量操作**和**事务**做准备。

**好处**:
1. **减少 RPC 次数**: 100 个 Put 可以合并为 1 次 RPC
2. **原子性保证**: 一批 Modify 要么全部成功，要么全部失败
3. **性能优化**: WAL 可以批量刷盘，减少 IO

**示例**:
```go
// 低效写法
for i := 0; i < 100; i++ {
    RawPut(ctx, key[i], value[i])  // 100 次 RPC
}

// 高效写法
modifies := []storage.Modify{}
for i := 0; i < 100; i++ {
    modifies = append(modifies, storage.Modify{...})
}
storage.Write(ctx, modifies)  // 1 次 RPC
```

---

### Q4: 如果在 RawScan 迭代过程中，有其他线程删除了当前迭代的 Key，会发生什么？

**A**: 这就是 MVCC 解决的问题。

**BadgerDB 的 MVCC 机制**:
- Reader 创建时获取一个**一致性快照**
- 快照基于事务开始时的版本号
- 后续写入不影响已创建的快照

```
T1: reader = Reader()     // 快照版本 = 1
T2: RawDelete(key)        // 版本变为 2
T3: iter.Next()           // 看到的仍是版本 1 的数据
```

**如果没有 MVCC**，会出现:
- 脏读：读到未提交的数据
- 幻读：同一次扫描结果不一致
- 不可重复读：两次读取结果不同

这也是为什么 Reader 必须 Close——长时间持有快照会阻止旧版本数据被清理。

---

### Q5: 如果让你设计一个支持分布式的 KV 存储，你会如何扩展这个架构？

**A**: 需要从以下几个层面扩展:

**1. 数据分片 (Sharding)**:
```go
// 增加路由层
type Router interface {
    GetRegion(key []byte) *Region
}

// RawGet 需要先查路由
func RawGet(key) {
    region := router.GetRegion(key)
    if region.Leader != local {
        // forward to leader
    }
    // 本地执行
}
```

**2. 共识协议 (Raft)**:
```go
// Storage 接口不变，实现从 StandAloneStorage 变为 RaftStorage
type RaftStorage struct {
    raftNode *raft.Node
    storage  Storage  // 底层仍然是 StandAloneStorage
}

func (r *RaftStorage) Write(modifies) {
    // 1. 提议到 Raft 日志
    r.raftNode.Propose(modifies)
    // 2. 等待多数派确认
    // 3. 应用到状态机 (调用底层 Storage.Write)
}
```

**3. 故障转移**:
```go
// 增加心跳和选举
func (r *RaftStorage) Start() {
    go r.runElectionTimer()
    go r.sendHeartbeats()
}
```

**关键点**: Storage 接口保持不变，通过替换实现从单机过渡到分布式。这正是**依赖倒置原则**的体现。

---

### Q6: BadgerDB 的 LSM-Tree 结构中，SSTable 和 MemTable 各有什么作用？

**A**: 这是 LSM-Tree 的核心数据结构。

```
Write Path:
WAL → MemTable (内存) → 刷盘 → SSTable (磁盘)
                              ↓
                        L0 → L1 → L2 → ... (Compaction)

Read Path:
MemTable → L0 SSTable → L1 SSTable → ... (Bloom Filter 加速)
```

**MemTable**:
- 内存中的有序表 (跳表/红黑树)
- 写入先写 MemTable，达到阈值后刷盘
- 优点：写入快；缺点：断电丢失

**SSTable (Sorted String Table)**:
- 磁盘上的有序文件
- 不可变，追加写入
- 多层结构，下层数据密度更高

**WAL (Write-Ahead Log)**:
- 持久化日志，用于恢复
- 先写 WAL，再写 MemTable
- MemTable 刷盘后可删除对应 WAL

这个设计实现了**写放大**和**读放大**的权衡：写入高效 (顺序写)，读取需要多层查找 (用 Bloom Filter 优化)。

---

## 四、总结与展望

### 1. 本次实现的收获

1. **理解了 KV 存储的基本接口设计**: Get/Put/Delete/Scan 是 KV 存储的基石
2. **掌握了 LSM-Tree 的使用**: 通过 BadgerDB 理解了现代 KV 存储的工作原理
3. **体会到了分层架构的价值**: Storage 抽象让后续扩展 Raft 变得容易
4. **认识了资源管理的重要性**: defer Close() 不是可有可无的

### 2. 可以改进的地方

1. **错误处理**: 当前直接返回 error，可以增加错误分类 (如 RegionNotFound)
2. **监控指标**: 添加延迟、QPS、错误率等指标
3. **连接池**: 管理 Badger 的 Transaction，避免频繁创建
4. **限流**: RawScan 需要限制扫描范围，防止全表扫描

### 3. 后续方向

- **Project 2**: 实现 Raft 共识，从单机到分布式
- **Project 3**: 实现事务层，支持 MVCC 和 ACID
- **Project 4**: 实现 Coprocessor，支持计算下推

---

*通过本次实验，我深刻体会到：一个好的架构设计，应该像水一样——接口清晰、分层合理、易于扩展。代码写得漂亮固然重要，但更重要的是理解背后的设计哲学和权衡取舍。*
