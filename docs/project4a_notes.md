# Project 4A 学习笔记 - MVCC 事务层

## 一、概述

Project 4A 实现 MVCC（多版本并发控制）事务层的核心功能，包括：
- 三 CF（Column Family）的数据读写
- 版本编码与时间戳处理
- 快照读与事务状态查询

---

## 二、数据链路

```
Client Request (Prewrite/Commit/Get)
    ↓
kv/server/server.go (gRPC handler)
    ↓
MvccTxn (kv/transaction/mvcc/transaction.go)
    ↓
Storage.Reader/Writer (kv/storage/)
    ↓
Badger KV Engine (底层存储)
```

**核心流程**：
- 输入：客户端请求携带 key/value/startTs/commitTs
- 处理：MvccTxn 将高层请求转换为底层 CF 的读写操作
- 输出：写入 Modify 列表，批量提交到 Storage

---

## 三、MVCC 三 CF 结构

| CF | Key 格式 | Value 内容 | 作用 |
|---|---|---|---|
| **Lock** | `user_key`（无时间戳） | `{primary, startTs, kind, ttl}` | 记录正在进行的事务，防止读写冲突 |
| **Default** | `EncodeKey(user_key, startTs)` | `value` | 存储实际数据，多版本 |
| **Write** | `EncodeKey(user_key, commitTs)` | `{kind, startTs}` | 记录事务状态，作为版本索引 |

**关键设计**：
- Lock 是全局的（不带时间戳），同一 key 只能有一个锁
- Default 用 startTs 编码，Write 用 commitTs 编码
- Write CF 按 commitTs 降序排列，扫描时第一个匹配的就是最新版本

---

## 四、核心函数实现

### 4.1 GetValue - 快照读

**目的**：读取 commitTs ≤ startTs 的最新已提交数据

**流程**：
```
1. Seek(EncodeKey(key, startTs)) → 定位到 Write CF
2. DecodeUserKey + Equal → 确认是目标 key
3. ParseWrite → 得到 {Kind, StartTS}
4. Kind == Put → 从 Default CF 取值
   Kind == Delete/Rollback → 返回 nil
```

**关键点**：
- Write CF 按 commitTs 降序，Seek 后第一个匹配的就是最新可见版本
- 必须检查 userKey，防止 Seek 跳到下一个 key

### 4.2 CurrentWrite - 查询当前事务状态

**目的**：判断事务是否已有最终状态（已提交或已回滚）

**流程**：
```
1. Seek(EncodeKey(key, ^uint64(0))) → 定位到最新 Write
2. 遍历，找到 write.StartTS == txn.StartTS 的 Write
3. 返回 Write 和 commitTs
```

**用途**：
- Commit：检查是否已提交，防止重复提交
- Rollback：检查是否已回滚，避免无效操作
- CheckTxnStatus：查询事务最终状态

### 4.3 MostRecentWrite - 查询最新 Write

**目的**：获取 key 的最新提交状态（用于冲突检查）

**流程**：
```
1. Seek(EncodeKey(key, ^uint64(0))) → 定位最新 Write
2. 检查 userKey 匹配
3. 返回 Write 和 commitTs
```

**用途**：Prewrite 检查是否有已提交数据覆盖

### 4.4 PutValue / DeleteValue

**目的**：向 Default CF 写入/删除数据

**Key 编码**：`EncodeKey(key, txn.StartTS)` — 用 startTs

### 4.5 PutWrite

**目的**：向 Write CF 写入事务状态

**Key 编码**：`EncodeKey(key, ts)` — 用 commitTs

### 4.6 GetLock / PutLock / DeleteLock

**目的**：操作 Lock CF

**Key 编码**：直接用 user_key，不带时间戳

---

## 五、关键设计理解

### 5.1 EncodeKey 的 ^ts 设计

```go
func EncodeKey(key []byte, ts uint64) []byte {
    binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
}
```

**原理**：
- `^ts` 按位取反，让大 ts 变成小数值，字节序排在前面
- 结果：commitTs 降序排列，扫描时第一个匹配的是最新版本
- 例如：`EncodeKey("a", 100)` 排在 `EncodeKey("a", 50)` 前面

### 5.2 为什么 Default 用 startTs，Write 用 commitTs

**Prewrite 时**：
- 写入 Default：`key + startTs → value`
- 写入 Lock：`key → {startTs, ...}`

**Commit 时**：
- 写入 Write：`key + commitTs → {startTs, kind}`
- 删除 Lock

**读取时**：
- 先从 Write CF 找 commitTs ≤ startTs 的最新 Write
- 得到 Write.startTs
- 用 startTs 从 Default CF 取值

**本质**：startTs 标识数据版本，commitTs 标识提交时间（作为索引）

---

## 六、时间戳来源

**commitTs 由 Scheduler TSO 分配，客户端携带在请求中**：

```
Scheduler (TSO)
    ↓ GetTS()
Client (TinySQL)
    ↓ CommitRequest{start_version, commit_version}
TinyKV Server
    ↓ KvCommit()
```

TinyKV 本身不分配时间戳，只使用客户端携带的时间戳。

---

## 七、遇到的疑惑与解答

### 疑问1：rollback 和 prewrite 为什么会并发竞争？

**答**：rollback 是客户端**主动决策**，而非 prewrite 的被动响应。2PC 中客户端并行发送 prewrite 到多个节点，某个节点失败后决定回滚，发送 rollback。由于并行发送，rollback 可能比某些 prewrite 先到达，产生时序竞争。

### 疑问2：rollback 标记为什么不影响其他事务？

**答**：Write CF 的 key 编码包含 commitTs，rollback 标记用事务自己的 startTs（commitTs=startTs）。其他事务的 commitTs 不同，key 编码不同，读写时不会触碰到这个 rollback 标记。

**本质**：MVCC 版本隔离，rollback 标记写在 T1 的 key 上，T2 走的是 T2 的 key，两条路不交叉。

---

## 八、修改的代码

### transaction.go 中的实现

| 函数 | 实现逻辑 |
|---|---|
| `GetValue` | Seek + userKey检查 + ParseWrite + Default取值 |
| `CurrentWrite` | Seek(^uint64(0)) + 遍历找 startTs 匹配 |
| `MostRecentWrite` | Seek(^uint64(0)) + userKey检查 + 返回 |
| `PutValue` | Modify{Put, CfDefault, EncodeKey(key, startTs)} |
| `DeleteValue` | Modify{Delete, CfDefault, EncodeKey(key, startTs)} |
| `PutWrite` | Modify{Put, CfWrite, EncodeKey(key, ts), Write.ToBytes()} |
| `GetLock` | Reader.GetCF(CfLock, key) + ParseLock |
| `PutLock` | Modify{Put, CfLock, key, Lock.ToBytes()} |
| `DeleteLock` | Modify{Delete, CfLock, key} |

---

## 九、值得注意的点

1. **Write CF 降序排列**：Seek 后第一个匹配的就是最新版本，无需全量扫描

2. **userKey 检查必要性**：Seek 可能跳到下一个 key，必须验证

3. **Modify 批量提交**：所有写入先缓存到 txn.writes，最后统一提交

4. **Lock CF 不带时间戳**：锁是全局的，任何事务查同一个 user_key

5. **rollback 标记的 commitTs = startTs**：回滚时两时间戳相同

---

## 十、核心设计决策记录

- **Rollback 是客户端主动决策**：防止迟到 prewrite 破坏回滚结果，rollback 标记作为"墓碑"
- **MVCC 版本隔离**：不同事务的 key 编码不同，天然隔离
- **降序排列优化**：Seek + ^ts 让扫描效率最大化