# Project 4B 学习笔记

## 一、项目概述

Project 4B 实现 Percolator 模式的两阶段提交（2PC）事务：
- **KvGet**: 读取单个 key 的值
- **KvPrewrite**: 2PC 第一阶段，检查冲突并加锁
- **KvCommit**: 2PC 第二阶段，提交事务

---

## 二、核心概念

### 2.1 两阶段提交（2PC）

```
阶段 1: Prewrite（准备）
  ├─ 检查写入冲突（Write CF）
  ├─ 检查锁冲突（Lock CF）
  ├─ 写入数据（Default CF）
  └─ 加锁（Lock CF）

阶段 2: Commit（提交）
  ├─ 写入提交记录（Write CF）
  └─ 删除锁（Lock CF）
```

### 2.2 三种锁的区别

| 锁类型 | 位置 | 作用 | 持有者 |
|--------|------|------|--------|
| **Latches** | server.Latches（内存） | 防止本地并发写入 | 当前线程 |
| **Lock CF** | Badger 存储 | Percolator 事务锁 | 当前事务 |
| **Write CF** | Badger 存储 | 提交状态记录 | 已完成的事务 |

**为什么需要两种锁（Latches + Lock CF）？**

```
场景：两个线程同时 Prewrite 同一个 key

没有 Latches：
  t1: 线程A 检查 Lock CF → nil ✓
  t2: 线程B 检查 Lock CF → nil ✓  ← A还没写入！
  t3: 线程A 写入 Lock CF → {Ts=100}
  t4: 线程B 写入 Lock CF → {Ts=200} ← 覆盖了A的锁！
  结果：A的锁丢失

有 Latches：
  t1: 线程A 获取 Latches["key"] ✓
  t2: 线程B 获取 Latches["key"] → 等待...
  t3: 线程A 检查 → 写入 → 释放 Latches
  t4: 线程B 获取 Latches → 检查 → 发现A的锁 → 返回错误 ✓
```

**核心原理**：Lock CF 只能检测"已写入的锁"，无法检测"正在检查但还没写入的锁"。Latches 把"检查+写入"打包成原子操作。

### 2.3 MVCC 三 CF 结构

```
Lock CF:    {user_key} → {primary, startTs, kind, ttl}
            同一 key 只能有一个锁，不带时间戳

Default CF: {EncodeKey(user_key, startTs)} → {value}
            存储实际数据，多版本

Write CF:   {EncodeKey(user_key, commitTs)} → {kind, startTs}
            记录事务状态（Put/Del/Rollback），commitTs 降序
```

---

## 三、代码改动详解

### 3.1 KvGet

**文件**: `kv/server/server.go`

**改动**: 实现完整的 KvGet 逻辑

```go
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
    resp := &kvrpcpb.GetResponse{}

    // 1. 获取 Reader
    reader, err := server.storage.Reader(req.Context)
    if regionErr, ok := err.(*raft_storage.RegionError); ok {
        resp.RegionError = regionErr.RequestErr
        return resp, nil
    }
    defer reader.Close()

    // 2. 创建事务对象
    txn := mvcc.NewMvccTxn(reader, req.Version)

    // 3. 检查 Lock CF
    lock, err := txn.GetLock(req.Key)
    if regionErr, ok := err.(*raft_storage.RegionError); ok {
        resp.RegionError = regionErr.RequestErr
        return resp, nil
    }

    // 如果有锁且 lock.Ts <= req.Version，返回 Locked 错误
    if lock != nil && req.Version >= lock.Ts {
        resp.Error = &kvrpcpb.KeyError{
            Locked: &kvrpcpb.LockInfo{
                PrimaryLock: lock.Primary,
                LockVersion: lock.Ts,
                Key:         req.Key,
                LockTtl:     lock.Ttl,
            },
        }
        return resp, nil
    }

    // 4. 从 Write CF + Default CF 读取值
    value, err := txn.GetValue(req.Key)
    if err != nil {
        if regionErr, ok := err.(*raft_storage.RegionError); ok {
            resp.RegionError = regionErr.RequestErr
            return resp, nil
        }
        return nil, err
    }

    // 5. 设置响应
    if value == nil {
        resp.NotFound = true
    }
    resp.Value = value
    return resp, nil
}
```

**数据链路**:

```
客户端请求: Get(key="foo", version=200)
    ↓
Reader → MvccTxn → GetLock("foo")
    ↓
有锁？
 ├─ Yes + lock.Ts <= 200 → 返回 Locked 错误
 └─ No → GetValue("foo")
        ↓
        Write CF: 找 commitTs <= 200 的最新 Put/Del
        ↓
        Default CF: 用 Write.startTs 取值
        ↓
    返回 value 或 NotFound
```

**注意**: KvGet 是读操作，不需要 Latches。

---

### 3.2 KvPrewrite

**文件**: `kv/server/server.go`

**改动**: 实现两阶段提交第一阶段

```go
func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
    resp := &kvrpcpb.PrewriteResponse{}

    // 1. 获取 Reader
    reader, err := server.storage.Reader(req.Context)
    defer reader.Close()

    // 2. 创建事务对象
    txn := mvcc.NewMvccTxn(reader, req.StartVersion)

    var keyErrors []*kvrpcpb.KeyError

    // 3. 遍历所有 Mutation，检查冲突
    for _, operation := range req.Mutations {
        // 3.1 检查 Write CF（写入冲突）
        write, ts, err := txn.MostRecentWrite(operation.Key)
        if write != nil && ts >= req.StartVersion {
            // commitTs >= startTs → 有新提交 → 冲突
            keyErrors = append(keyErrors, &kvrpcpb.KeyError{
                Conflict: &kvrpcpb.WriteConflict{
                    StartTs:    req.StartVersion,
                    ConflictTs: ts,
                    Key:        operation.Key,
                    Primary:    req.PrimaryLock,
                },
            })
            continue
        }

        // 3.2 检查 Lock CF（是否被锁）
        lock, err := txn.GetLock(operation.Key)
        if lock != nil {
            // 有锁 → 被占用
            keyErrors = append(keyErrors, &kvrpcpb.KeyError{
                Locked: &kvrpcpb.LockInfo{
                    PrimaryLock: req.PrimaryLock,
                    LockVersion: lock.Ts,
                    Key:         operation.Key,
                    LockTtl:     lock.Ttl,
                },
            })
            continue
        }

        // 3.3 暂存修改到 txn
        var kind mvcc.WriteKind
        switch operation.Op {
        case kvrpcpb.Op_Put:
            kind = mvcc.WriteKindPut
            txn.PutValue(operation.Key, operation.Value)
        case kvrpcpb.Op_Del:
            kind = mvcc.WriteKindDelete
            txn.DeleteValue(operation.Key)
        }

        // 3.4 暂存锁到 txn
        txn.PutLock(operation.Key, &mvcc.Lock{
            Primary: req.PrimaryLock,
            Ts:      req.StartVersion,
            Ttl:     req.LockTtl,
            Kind:    kind,
        })
    }

    // 4. 有冲突则返回错误，不写入
    if len(keyErrors) > 0 {
        resp.Errors = keyErrors
        return resp, nil
    }

    // 5. 批量写入所有暂存数据
    err = server.storage.Write(req.Context, txn.Writes())
    return resp, nil
}
```

**数据链路**:

```
客户端请求: Prewrite(keys=["a","b"], primary="a", startTs=100)
    ↓
遍历每个 key:
    ├─ MostRecentWrite(key)
    │   └─ Write CF: 找最新 commitTs
    │       └─ commitTs >= startTs → 冲突！记录 KeyError.Conflict
    │
    ├─ GetLock(key)
    │   └─ Lock CF: 检查是否被锁
    │       └─ lock != nil → 被锁！记录 KeyError.Locked
    │
    └─ 无冲突 → 暂存
        ├─ PutValue/DeleteValue → txn.writes 列表
        └─ PutLock → txn.writes 列表
    ↓
所有 key 检查完毕
    ├─ 有 keyErrors → 返回错误列表，不写入
    └─ 无错误 → storage.Write(txn.Writes())
                ├─ Default CF: 数据值
                └─ Lock CF: 事务锁
```

**暂存设计的原因**:
1. **原子性**: 先检查所有 key，确认无冲突后再写入
2. **效率**: 批量写入比逐条写入更快
3. **一致性**: 避免部分写入导致数据不一致

**缺失**: 当前代码没有 Latches，存在竞态风险。应该添加：
```go
// 开头
keys := make([][]byte, len(req.Mutations))
for i, m := range req.Mutations {
    keys[i] = m.Key
}
latches := server.Latches.AcquireLatches(keys)

// 返回前
server.Latches.ReleaseLatches(latches)
```

---

### 3.3 KvCommit

**文件**: `kv/server/server.go`

**改动**: 实现两阶段提交第二阶段（多次修改后才通过测试）

**最终代码**:

```go
func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
    resp := &kvrpcpb.CommitResponse{}

    // 1. 获取 Reader
    reader, err := server.storage.Reader(req.Context)
    defer reader.Close()

    // 2. 创建事务对象
    txn := mvcc.NewMvccTxn(reader, req.StartVersion)

    // 3. 获取 Latches
    server.Latches.WaitForLatches(req.Keys)
    defer server.Latches.ReleaseLatches(req.Keys)

    for _, key := range req.Keys {
        // 4. 检查 Write CF
        write, _, err := txn.CurrentWrite(key)
        if err != nil {
            // 处理错误...
        }

        if write != nil {
            if write.Kind == mvcc.WriteKindRollback {
                // 已 Rollback → 返回 Retryable 错误
                resp.Error = &kvrpcpb.KeyError{Retryable: "true"}
                return resp, nil
            }
            // 已 Commit → 返回 nil（幂等）
            return resp, nil
        }

        // 5. 检查 Lock CF
        lock, err := txn.GetLock(key)
        if err != nil {
            // 处理错误...
        }

        if lock == nil {
            // Lock 不存在（Prewrite 丢失）→ 返回 nil
            return resp, nil
        }

        if lock.Ts != req.StartVersion {
            // 其他事务的锁 → 返回 Retryable 错误
            resp.Error = &kvrpcpb.KeyError{Retryable: "true"}
            return resp, nil
        }

        // 6. 正常提交
        txn.PutWrite(key, req.CommitVersion, &mvcc.Write{
            StartTS: req.StartVersion,
            Kind:    lock.Kind,
        })
        txn.DeleteLock(key)
    }

    // 7. 批量写入
    err = server.storage.Write(req.Context, txn.Writes())
    return resp, nil
}
```

**数据链路**:

```
客户端请求: Commit(keys=["a"], startTs=100, commitTs=150)
    ↓
获取 Latches(keys)
    ↓
遍历每个 key:
    ├─ CurrentWrite(key)
    │   └─ Write CF: 找 startTs=100 的记录
    │       ├─ Rollback → 返回 Retryable 错误
    │       ├─ Commit → 返回 nil（幂等）
    │       └─ 空 → 继续
    │
    ├─ GetLock(key)
    │   └─ Lock CF:
    │       ├─ nil → Prewrite 丢失 → 返回 nil
    │       ├─ Ts≠100 → 其他事务的锁 → 返回 Retryable
    │       └─ Ts=100 → 正常，继续
    │
    └─ 暂存
        ├─ PutWrite → Write CF: {commitTs=150, startTs=100}
        └─ DeleteLock → 删除 Lock CF
    ↓
释放 Latches
    ↓
storage.Write(txn.Writes())
```

---

## 四、遇到的问题与解决

### 4.1 TestCommitMissingPrewrite4B 失败

**问题描述**: Commit 一个没有 Prewrite 的 key，期望返回 nil，实际返回 Retryable 错误。

**原因分析**:

原代码：
```go
if lock == nil || lock.Ts != req.StartVersion {
    resp.Error = &kvrpcpb.KeyError{Retryable: "true"}  // 错误！
    return resp, nil
}
```

**问题**: Prewrite 丢失（Lock CF 空 + Write CF 空）时，返回 Retryable 让客户端无意义重试。正确行为是返回 nil，让客户端认为"事务已完成"并开始新事务。

**解决**: Lock 不存在时返回 nil，而不是 Retryable。

### 4.2 TestCommitConflictRollback4B 失败

**问题描述**: Commit 一个已 Rollback 的 key，期望返回 Retryable 错误，实际返回 nil。

**原因分析**:

原代码：
```go
if write != nil && write.Kind != mvcc.WriteKindRollback && write.StartTS == req.StartVersion {
    return resp, nil  // Rollback 时条件不满足，继续执行
}
```

**问题**: Rollback 类型的 Write 没被正确处理，导致继续执行后续逻辑。

**解决**: 明确区分 Rollback 和 Commit：
```go
if write != nil {
    if write.Kind == mvcc.WriteKindRollback {
        resp.Error = &kvrpcpb.KeyError{Retryable: "true"}
        return resp, nil
    }
    return resp, nil  // 已 Commit，幂等返回
}
```

### 4.3 最终判断逻辑

| 场景 | Write CF | Lock CF | 返回值 |
|------|----------|---------|--------|
| 已 Commit | Commit 记录 | - | nil（幂等） |
| 已 Rollback | Rollback 记录 | - | Retryable 错误 |
| Prewrite 丢失 | 空 | 空 | nil |
| 其他事务的锁 | 空 | Ts≠当前事务 | Retryable 错误 |
| 正常提交 | 空 | Ts=当前事务 | 执行提交 |

---

## 五、不清楚的地方

### 5.1 LockInfo.PrimaryLock 应该是谁？

在 KvPrewrite 返回 Locked 错误时：
```go
keyErrors = append(keyErrors, &kvrpcpb.KeyError{
    Locked: &kvrpcpb.LockInfo{
        PrimaryLock: req.PrimaryLock,  // 当前事务的 Primary？
        // 还是应该用 lock.Primary（锁持有者的 Primary）？
    },
})
```

**疑问**: 应该返回当前事务的 Primary，还是锁持有者的 Primary？

**理解**: 应该返回 `lock.Primary`（锁持有者的），因为客户端需要通过 Primary 判断锁持有者的状态（ResolveLock）。

### 5.2 为什么 Rollback 后不能 Commit？

Percolator 设计：Rollback 是"事务失败"的标记，代表事务已明确放弃。此时再 Commit 会违反事务语义：

```
时间线：
  t1: Prewrite(key, startTs=100)
  t2: Rollback(key, startTs=100) → Write CF: {kind=Rollback}
  t3: Commit(key, startTs=100) → 应该拒绝！

原因：Rollback 意味着"这个事务已放弃"，不能再"成功提交"
```

### 5.3 Prewrite 丢失为什么不报错？

Prewrite 请求丢失 = 事务已失败。返回 nil 的原因：

```
1. 客户端收到 nil → 认为事务"已完成"
2. 客户端开始新事务 → 正常处理后续业务
3. 如果返回 Retryable → 客户端会无限重试一个已失败的事务
```

**设计哲学**: 让客户端"忘记"失败的事务，重新开始。

---

## 六、可以优化的地方

### 6.1 KvPrewrite 缺少 Latches

**当前状态**: 没有本地并发锁，存在竞态风险。

**优化方案**:
```go
func (server *Server) KvPrewrite(...) {
    // 开头添加
    keys := make([][]byte, len(req.Mutations))
    for i, m := range req.Mutations {
        keys[i] = m.Key
    }
    latches := server.Latches.AcquireLatches(keys)
    defer server.Latches.ReleaseLatches(latches)

    // 原有逻辑...
}
```

### 6.2 错误处理可以更精细

当前代码用 `Retryable: "true"` 作为通用错误，可以更具体：

```go
// 当前
resp.Error = &kvrpcpb.KeyError{Retryable: "true"}

// 可以更具体
resp.Error = &kvrpcpb.KeyError{
    Retryable: "true",
    Abort: "transaction already rolled back",  // 更详细的描述
}
```

### 6.3 批量写入的原子性

当前 `storage.Write(txn.Writes())` 是一次性写入，但如果中途失败，部分数据可能已写入。

**潜在问题**: 写入 Default CF 成功但 Lock CF 失败 → 数据不一致。

**优化思路**: 使用 Badger 事务保证原子性（可能已实现，需确认）。

---

## 七、有价值的学习点

### 7.1 暂存模式（Staging）

**设计**: 先暂存到内存列表，最后批量写入。

**好处**:
1. 原子性：先检查所有 key，无冲突再写入
2. 效率：减少 IO 操作
3. 回滚方便：写入前发现冲突，直接丢弃暂存列表

**类比**: 购物车模式 — 先把商品放进购物车（暂存），结账时一次性支付（批量写入）。

### 7.2 幂等性设计

**场景**: 同一个 Commit 请求可能被发送多次（网络重试）。

**解决**: 检查 Write CF，如果已有记录则直接返回 nil。

```go
if write != nil && write.Kind != mvcc.WriteKindRollback {
    return resp, nil  // 已处理，不再重复写入
}
```

### 7.3 锁的层级设计

| 层级 | 锁类型 | 保护范围 | 持有时间 |
|------|--------|----------|----------|
| 线程 | Latches | 本地并发 | 检查+写入期间 |
| 事务 | Lock CF | 分布式事务 | Prewrite→Commit |

**设计哲学**: 不同层级用不同锁，各司其职。

### 7.4 MVCC 读取流程

```
GetValue(key, startTs):
  1. Lock CF 检查 → 有锁则报错
  2. Write CF 找 commitTs <= startTs 的最新 Put/Del
  3. Default CF 用 Write.startTs 取值
```

**理解**: Write CF 是"版本索引"，Default CF 是"数据仓库"。

---

## 八、测试覆盖的场景

| 测试名 | 场景 | 验证点 |
|--------|------|--------|
| TestGetValue4B | 正常读取 | 能读到值 |
| TestGetLocked4B | 读到锁住的 key | 返回 Locked 错误 |
| TestSinglePrewrite4B | 正常 Prewrite | 写入 Default + Lock |
| TestPrewriteLocked4B | Prewrite 遇到锁 | 返回 Locked 错误 |
| TestPrewriteWritten4B | Prewrite 遇到新写入 | 返回 Conflict 错误 |
| TestSingleCommit4B | 正常 Commit | 写入 Write + 删除 Lock |
| TestRecommitKey4B | 重复 Commit | 幂等返回 nil |
| TestCommitConflictRollback4B | Commit 已 Rollback 的 key | 返回 Retryable |
| TestCommitMissingPrewrite4B | Commit 无 Prewrite 的 key | 返回 nil |

---

## 九、总结

Project 4B 核心是实现 Percolator 的两阶段提交：

1. **Prewrite**: 检查冲突 → 写数据 → 加锁
2. **Commit**: 写提交记录 → 删除锁

**关键设计**:
- Latches: 本地并发锁，防止竞态
- Lock CF: 分布式事务锁，标记占用
- Write CF: 提交状态记录，幂等判断
- 暂存模式: 先检查后写入，保证原子性

**教训**: 不同场景需要不同处理，特别是"已 Rollback"和"Prewrite 丢失"的区别。