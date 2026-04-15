# Project 1 总结

## 实现内容

### 1. StandaloneStorage 实现

在 `kv/storage/standalone_storage/standalone_storage.go` 中实现了基于 BadgerDB 的存储引擎：

- **数据结构**：使用 `engine_util.Engines` 管理 KV 和 Raft 两个 Badger 实例
- **NewStandAloneStorage**：初始化 KV 和 Raft 两个数据库实例
- **Start/Stop**：启动和关闭存储引擎
- **Write**：将修改批量写入存储
- **Reader**：创建只读事务用于读取操作

### 2. Raw API 实现

在 `kv/server/raw_api.go` 中实现了四个 RPC 接口：

| 接口 | 功能 |
|------|------|
| RawGet | 根据 CF 和 Key 获取值 |
| RawPut | 写入键值对 |
| RawDelete | 删除键值对 |
| RawScan | 扫描指定范围内的键值对 |

**关键点**：
- 每次读取操作都需要创建 `Reader`
- 使用 `defer reader.Close()` 确保事务被正确关闭
- 使用 `defer iter.Close()` 确保迭代器被正确关闭

## Windows 测试问题

在 Windows 上运行测试时可能会遇到文件锁错误：

```
The process cannot access the file because it is being used by another process.
```

**原因**：BadgerDB 使用内存映射文件（mmap），在 Windows 上文件锁释放较慢。

**解决方案**：
1. 在 Mac/Linux 上运行测试（推荐）
2. 每次测试前手动清理：`rm -rf /tmp/badger`
3. 确保代码中正确关闭所有资源（`reader.Close()`、`iter.Close()`）

## 运行测试

```bash
# 运行单个测试
CGO_ENABLED=1 go test -v -run TestRawGet1 ./kv/server

# 运行所有 Project1 测试
make project1
```
