# Project2 RaftKV

Raft 是一种旨在易于理解的共识算法。你可以在 [Raft 官网](https://raft.github.io/) 阅读关于 Raft 的资料，包括 Raft 的交互式可视化演示和其他资源，包括 [Raft 扩展论文](https://raft.github.io/raft.pdf)。

在这个项目中，你将基于 Raft 构建一个高可用的 KV 服务器。这不仅需要你实现 Raft 算法，还需要将其实际应用起来，这将带来更多挑战，比如使用 `badger` 管理 Raft 的持久化状态、为快照消息添加流控等。

本项目分为三个部分：

- 实现基础 Raft 算法
- 在 Raft 之上构建容错 KV 服务器
- 添加 Raft 日志 GC 和快照支持

## Part A

### 代码说明

在这一部分，你将实现基础的 Raft 算法。需要实现的代码位于 `raft/` 目录下。`raft/` 目录中已经有一些骨架代码和测试用例等着你。这里实现的 Raft 算法与上层应用有着设计良好的接口。此外，它使用逻辑时钟（这里称为 tick）来衡量选举和心跳超时，而不是物理时钟。也就是说，你不需要在 Raft 模块内部设置定时器，上层应用负责通过调用 `RawNode.Tick()` 来推进逻辑时钟。除此之外，消息的发送和接收以及其他事情都是异步处理的，何时实际执行这些操作也由上层应用决定（详见下文）。例如，Raft 不会阻塞等待任何请求消息的响应。

在开始实现之前，请先查看这一部分的提示。另外，你应该粗略浏览一下 proto 文件 `proto/proto/eraftpb.proto`。Raft 发送和接收消息的相关结构体都在那里定义，实现时会用到它们。注意，与 Raft 论文不同，这里将 Heartbeat 和 AppendEntries 分成不同的消息，以使逻辑更清晰。

这一部分可以分解为 3 个步骤：

- Leader 选举
- 日志复制
- RawNode 接口实现

### 实现 Raft 算法

`raft/raft.go` 中的 `raft.Raft` 提供了 Raft 算法的核心功能，包括消息处理、驱动逻辑时钟等。更多实现指导请查看 `raft/doc.go`，其中包含设计概览以及这些 `MessageTypes` 的职责说明。

#### Leader 选举

要实现 Leader 选举，可以从 `raft.Raft.tick()` 开始。它用于将内部逻辑时钟推进一个 tick，从而驱动选举超时或心跳超时。你现在不需要关心消息发送和接收的逻辑。如果需要发出消息，只需将其推送到 `raft.Raft.msgs`，所有 Raft 接收到的消息都会传递给 `raft.Raft.Step()`。测试代码会从 `raft.Raft.msgs` 获取消息，并通过 `raft.Raft.Step()` 传递响应消息。`raft.Raft.Step()` 是消息处理的入口，你应该处理 `MsgRequestVote`、`MsgHeartbeat` 及其响应消息。同时请实现测试中的辅助函数并正确调用它们，比如 `raft.Raft.becomeXXX`，用于在 Raft 角色变化时更新 Raft 内部状态。

你可以运行 `make project2aa` 来测试实现，并查看这一部分结束时的一些提示。

#### 日志复制

要实现日志复制，可以从处理 `MsgAppend` 和 `MsgAppendResponse` 开始，包括发送方和接收方两侧。查看 `raft/log.go` 中的 `raft.RaftLog`，它是一个帮助你管理 Raft 日志的辅助结构。在这里，你还需要通过 `raft/storage.go` 中定义的 `Storage` 接口与上层应用交互，获取持久化数据如日志条目和快照。

你可以运行 `make project2ab` 来测试实现，并查看这一部分结束时的一些提示。

### 实现 RawNode 接口

`raft/rawnode.go` 中的 `raft.RawNode` 是我们与上层应用交互的接口。`raft.RawNode` 包含 `raft.Raft` 并提供一些包装函数，如 `RawNode.Tick()` 和 `RawNode.Step()`。它还提供 `RawNode.Propose()` 让上层应用提出新的 Raft 日志。

另一个重要的结构体 `Ready` 也在这里定义。当处理消息或推进逻辑时钟时，`raft.Raft` 可能需要与上层应用交互，例如：

- 发送消息给其他 Peer
- 保存日志条目到稳定存储
- 保存硬状态（如 term、commit index、vote）到稳定存储
- 将已提交的日志条目应用到状态机
- 等等

但这些交互不会立即发生，而是封装在 `Ready` 中，通过 `RawNode.Ready()` 返回给上层应用。上层应用何时调用 `RawNode.Ready()` 并处理它，完全由上层应用决定。在处理完返回的 `Ready` 后，上层应用还需要调用一些函数（如 `RawNode.Advance()`）来更新 `raft.Raft` 的内部状态，如 applied index、stabled log index 等。

你可以运行 `make project2ac` 来测试实现，运行 `make project2a` 来测试整个 Part A。

> 提示：
>
> - 在 `raft.Raft`、`raft.RaftLog`、`raft.RawNode` 和 `eraftpb.proto` 的消息中添加你需要的任何状态
> - 测试假设首次启动 Raft 时 term 应为 0
> - 测试假设新选举的 Leader 应该在其 term 上追加一条 noop 条目
> - 测试假设一旦 Leader 推进其 commit index，就会通过 `MessageType_MsgAppend` 消息广播 commit index
> - 测试不为本地消息设置 term：`MessageType_MsgHup`、`MessageType_MsgBeat` 和 `MessageType_MsgPropose`
> - Leader 和 Non-Leader 追加日志条目的方式非常不同，有不同的来源、检查和处理，要小心
> - 不要忘记不同 Peer 之间的选举超时应该不同
> - `rawnode.go` 中的一些包装函数可以通过 `raft.Step(local message)` 实现
> - 启动新的 Raft 时，从 `Storage` 获取最后稳定的状态来初始化 `raft.Raft` 和 `raft.RaftLog`

## Part B

在这一部分，你将使用 Part A 中实现的 Raft 模块构建一个容错键值存储服务。你的 KV 服务将是一个复制状态机，由多个使用 Raft 进行复制的 KV 服务器组成。只要大多数服务器存活且能够通信，你的 KV 服务就应该继续处理客户端请求，即使存在其他故障或网络分区。

在 Project1 中，你已经实现了一个独立的 KV 服务器，所以你应该已经熟悉 KV 服务器 API 和 `Storage` 接口。

在介绍代码之前，你需要先理解三个术语，它们定义在 `proto/proto/metapb.proto` 中：

- Store 代表一个 tinykv-server 实例
- Peer 代表运行在 Store 上的一个 Raft 节点
- Region 是 Peer 的集合，也称为 Raft 组

![region](imgs/region.png)

为简单起见，Project2 中一个 Store 上只有一个 Peer，一个集群中只有一个 Region。所以你现在不需要考虑 Region 的范围。多个 Region 将在 Project3 中进一步引入。

### 代码说明

首先，你应该查看 `kv/storage/raft_storage/raft_server.go` 中的 `RaftStorage`，它也实现了 `Storage` 接口。与直接读写底层引擎的 `StandaloneStorage` 不同，它将每个写和读请求先发送给 Raft，然后在 Raft 提交请求后才实际对底层引擎进行读写。通过这种方式，它可以保持多个 Store 之间的一致性。

`RaftStorage` 创建一个 `Raftstore` 来驱动 Raft。当调用 `Reader` 或 `Write` 函数时，它实际上通过 channel（`raftWorker` 的 `raftCh`）向 raftstore 发送一个定义在 `proto/proto/raft_cmdpb.proto` 中的 `RaftCmdRequest`，其中包含四种基本命令类型（Get/Put/Delete/Snap），并在 Raft 提交并应用命令后返回响应。`Reader` 和 `Write` 函数的 `kvrpc.Context` 参数现在很有用，它携带了从客户端角度看的 Region 信息，并作为 `RaftCmdRequest` 的 header 传递。这些信息可能不正确或过时，所以 raftstore 需要检查它们并决定是否提出请求。

然后，这里来到了 TinyKV 的核心——raftstore。结构有点复杂，阅读 TiKV 参考文章以更好地理解设计：

- <https://pingcap.com/blog-cn/the-design-and-implementation-of-multi-raft/#raftstore> (中文版)
- <https://pingcap.com/blog/design-and-implementation-of-multi-raft/#raftstore> (英文版)

Raftstore 的入口是 `Raftstore`，见 `kv/raftstore/raftstore.go`。它启动一些 worker 来异步处理特定任务，其中大多数现在还没有用到，所以你可以忽略它们。你需要关注的是 `raftWorker`（`kv/raftstore/raft_worker.go`）。

整个过程分为两部分：raft worker 轮询 `raftCh` 获取消息，包括驱动 Raft 模块的基础 tick 和要作为 Raft 条目提出的 Raft 命令；它从 Raft 模块获取并处理 ready，包括发送 Raft 消息、持久化状态、将已提交的条目应用到状态机。一旦应用完成，就向客户端返回响应。

### 实现 Peer Storage

Peer Storage 是你在 Part A 中通过 `Storage` 接口交互的组件，但除了 Raft 日志外，Peer Storage 还管理其他持久化的元数据，这些元数据对于在重启后恢复一致的状态机非常重要。此外，`proto/proto/raft_serverpb.proto` 中定义了三个重要的状态：

- RaftLocalState：用于存储当前 Raft 的 HardState 和最后一条日志索引
- RaftApplyState：用于存储 Raft 应用的最后日志索引和一些截断的日志信息
- RegionLocalState：用于存储 Region 信息和该 Store 上对应的 Peer 状态。Normal 表示此 Peer 正常，Tombstone 表示此 Peer 已从 Region 中移除，不能再加入 Raft 组

这些状态存储在两个 badger 实例中：raftdb 和 kvdb：

- raftdb 存储 Raft 日志和 `RaftLocalState`
- kvdb 存储不同列族中的键值数据、`RegionLocalState` 和 `RaftApplyState`。你可以将 kvdb 视为 Raft 论文中提到的状态机

格式如下，`kv/raftstore/meta` 中提供了一些辅助函数，并通过 `writebatch.SetMeta()` 将它们设置到 badger 中。

| Key              | KeyFormat                        | Value            | DB   |
| :--------------- | :------------------------------- | :--------------- | :--- |
| raft_log_key     | 0x01 0x02 region_id 0x01 log_idx | Entry            | raft |
| raft_state_key   | 0x01 0x02 region_id 0x02         | RaftLocalState   | raft |
| apply_state_key  | 0x01 0x02 region_id 0x03         | RaftApplyState   | kv   |
| region_state_key | 0x01 0x03 region_id 0x01         | RegionLocalState | kv   |

> 你可能想知道为什么 TinyKV 需要两个 badger 实例。实际上，它可以只使用一个 badger 来存储 Raft 日志和状态机数据。分成两个实例只是为了与 TiKV 设计保持一致。

这些元数据应该在 `PeerStorage` 中创建和更新。创建 PeerStorage 时，见 `kv/raftstore/peer_storage.go`。它初始化此 Peer 的 RaftLocalState、RaftApplyState，或者在重启情况下从底层引擎获取之前的值。注意，RAFT_INIT_LOG_TERM 和 RAFT_INIT_LOG_INDEX 的值都是 5（只要大于 1 即可），而不是 0。不设置为 0 的原因是为了与配置变更后被动创建的 Peer 情况区分开来。你现在可能不太理解这一点，所以先记住它，细节将在 Project3b 实现配置变更时描述。

这一部分你只需要实现一个函数：`PeerStorage.SaveReadyState`。这个函数的作用是将 `raft.Ready` 中的数据保存到 badger，包括追加日志条目和保存 Raft 硬状态。

要追加日志条目，只需将 `raft.Ready.Entries` 中的所有日志条目保存到 raftdb，并删除任何之前追加的永远不会被提交的日志条目。同时，更新 PeerStorage 的 `RaftLocalState` 并保存到 raftdb。

保存硬状态也很简单，只需更新 PeerStorage 的 `RaftLocalState.HardState` 并保存到 raftdb。

> 提示：
>
> - 使用 `WriteBatch` 一次性保存这些状态
> - 查看 `peer_storage.go` 中的其他函数，了解如何读写这些状态
> - 设置环境变量 LOG_LEVEL=debug，这可能有助于调试，另见所有可用的 [日志级别](../log/log.go)

### 实现 Raft Ready 处理

在 Project2 Part A 中，你已经构建了一个基于 tick 的 Raft 模块。现在你需要编写外部流程来驱动它。大部分代码已经在 `kv/raftstore/peer_msg_handler.go` 和 `kv/raftstore/peer.go` 中实现。所以你需要学习代码并完成 `proposeRaftCommand` 和 `HandleRaftReady` 的逻辑。以下是对框架的一些解释。

Raft `RawNode` 已经与 `PeerStorage` 一起创建并存储在 `peer` 中。在 raft worker 中，可以看到它获取 `peer` 并用 `peerMsgHandler` 包装它。`peerMsgHandler` 主要有两个功能：一个是 `HandleMsg`，另一个是 `HandleRaftReady`。

`HandleMsg` 处理从 raftCh 接收到的所有消息，包括调用 `RawNode.Tick()` 驱动 Raft 的 `MsgTypeTick`、封装客户端请求的 `MsgTypeRaftCmd` 以及在 Raft Peer 之间传输的 `MsgTypeRaftMessage`。所有消息类型都定义在 `kv/raftstore/message/msg.go` 中。你可以查看详细信息，其中一些将在后续部分使用。

处理完消息后，Raft 节点应该有一些状态更新。所以 `HandleRaftReady` 应该从 Raft 模块获取 ready 并执行相应的操作，如持久化日志条目、应用已提交的条目、通过网络向其他 Peer 发送 Raft 消息。

用伪代码表示，raftstore 使用 Raft 的方式如下：

```go
for {
  select {
  case <-s.Ticker:
    Node.Tick()
  default:
    if Node.HasReady() {
      rd := Node.Ready()
      saveToStorage(rd.State, rd.Entries, rd.Snapshot)
      send(rd.Messages)
      for _, entry := range rd.CommittedEntries {
        process(entry)
      }
      s.Node.Advance(rd)
    }
  }
}
```

在此之后，整个读或写的流程如下：

- 客户端调用 RPC RawGet/RawPut/RawDelete/RawScan
- RPC handler 调用 `RaftStorage` 相关方法
- `RaftStorage` 向 raftstore 发送一个 Raft 命令请求，并等待响应
- `RaftStore` 将 Raft 命令请求作为 Raft 日志提出
- Raft 模块追加日志，并通过 `PeerStorage` 持久化
- Raft 模块提交日志
- Raft worker 在处理 Raft ready 时执行 Raft 命令，并通过 callback 返回响应
- `RaftStorage` 从 callback 接收响应并返回给 RPC handler
- RPC handler 执行一些操作并返回 RPC 响应给客户端

你应该运行 `make project2b` 来通过所有测试。整个测试运行一个模拟集群，包括多个带有模拟网络的 TinyKV 实例。它执行一些读写操作，并检查返回值是否符合预期。

需要注意的是，错误处理是通过测试的重要部分。你可能已经注意到，`proto/proto/errorpb.proto` 中定义了一些错误，并且错误是 gRPC 响应的一个字段。同样，实现 `error` 接口的相应错误定义在 `kv/raftstore/util/error.go` 中，所以你可以将它们作为函数的返回值使用。

这些错误主要与 Region 相关。所以它也是 `RaftCmdResponse` 的 `RaftResponseHeader` 的成员。在提出请求或应用命令时，可能会出现一些错误。如果是这样，你应该返回带有错误的 Raft 命令响应，然后错误将进一步传递给 gRPC 响应。你可以使用 `kv/raftstore/cmd_resp.go` 中提供的 `BindRespError` 在返回响应时将这些错误转换为 `errorpb.proto` 中定义的错误。

在这个阶段，你可能只需要考虑这些错误，其他错误将在 Project3 中处理：

- ErrNotLeader：Raft 命令是在 Follower 上提出的。所以用它来让客户端尝试其他 Peer
- ErrStaleCommand：这可能是由于 Leader 变更，某些日志没有被提交并被新 Leader 的日志覆盖。但客户端不知道这一点，仍在等待响应。所以你应该返回这个错误让客户端知道并重试命令

> 提示：
>
> - `PeerStorage` 实现了 Raft 模块的 `Storage` 接口，你应该使用提供的方法 `SaveReadyState()` 来持久化 Raft 相关状态
> - 使用 `engine_util` 中的 `WriteBatch` 原子地进行多次写入，例如，你需要确保在一次写入批次中应用已提交的条目并更新 applied index
> - 使用 `Transport` 向其他 Peer 发送 Raft 消息，它在 `GlobalContext` 中
> - 如果服务器不是大多数节点的一部分且没有最新数据，它不应该完成 get RPC。你可以直接将 get 操作放入 Raft 日志中，或者实现 Raft 论文第 8 节中描述的只读操作优化
> - 应用日志条目时不要忘记更新并持久化 apply state
> - 你可以像 TiKV 那样以异步方式应用已提交的 Raft 日志条目。这不是必需的，虽然是一个提高性能的大挑战
> - 提出命令时记录 callback，并在应用后返回 callback
> - 对于 snap 命令响应，应该显式地将 badger Txn 设置到 callback
> - 在 2A 之后，一些测试你可能需要运行多次才能找到 bug

## Part C

就目前代码的情况而言，对于长期运行的服务器来说，永远记住完整的 Raft 日志是不现实的。相反，服务器会检查 Raft 日志的数量，并不时丢弃超过阈值的日志条目。

在这一部分，你将基于前两个部分的实现来添加快照处理。一般来说，快照只是一个像 AppendEntries 一样用于向 Follower 复制数据的 Raft 消息，但它的不同之处在于其大小。快照包含某个时间点的整个状态机数据，一次性构建和发送这么大的消息会消耗很多资源和时间，可能会阻塞其他 Raft 消息的处理。为了分摊这个问题，快照消息将使用独立的连接，并将数据分成块进行传输。这就是 TinyKV 服务有快照 RPC API 的原因。如果你对发送和接收的细节感兴趣，请查看 `snapRunner` 和参考文章 <https://pingcap.com/blog-cn/tikv-source-code-reading-10/>

### 代码说明

你需要在 Part A 和 Part B 编写的代码基础上进行修改。

### 在 Raft 中实现

虽然我们需要对快照消息进行一些不同的处理，但从 Raft 算法的角度来看应该没有区别。查看 proto 文件中 `eraftpb.Snapshot` 的定义，`eraftpb.Snapshot` 的 `data` 字段并不代表实际的状态机数据，而是一些元数据，供上层应用使用，你现在可以忽略它。当 Leader 需要向 Follower 发送快照消息时，它可以调用 `Storage.Snapshot()` 获取 `eraftpb.Snapshot`，然后像其他 Raft 消息一样发送快照消息。状态机数据实际如何构建和发送是由 raftstore 实现的，将在下一步介绍。你可以假设一旦 `Storage.Snapshot()` 成功返回，Raft Leader 就可以安全地向 Follower 发送快照消息，而 Follower 应该调用 `handleSnapshot` 来处理它，即从消息中的 `eraftpb.SnapshotMetadata` 恢复 Raft 内部状态，如 term、commit index 和成员信息等。之后，快照处理过程就完成了。

### 在 Raftstore 中实现

在这一步，你需要学习 raftstore 的另外两个 worker——raftlog-gc worker 和 region worker。

Raftstore 根据配置 `RaftLogGcCountLimit` 定期检查是否需要 GC 日志，见 `onRaftGcLogTick()`。如果是，它将提出一个 Raft 管理命令 `CompactLogRequest`，该命令封装在 `RaftCmdRequest` 中，就像 Project2 Part B 中实现的四种基本命令类型（Get/Put/Delete/Snap）一样。然后你需要在 Raft 提交时处理这个管理命令。但与 Get/Put/Delete/Snap 命令读写状态机数据不同，`CompactLogRequest` 修改元数据，即更新 `RaftApplyState` 中的 `RaftTruncatedState`。之后，你应该通过 `ScheduleCompactLog` 向 raftlog-gc worker 调度一个任务。Raftlog-gc worker 将异步执行实际的日志删除工作。

由于日志压缩，Raft 模块可能需要发送快照。`PeerStorage` 实现了 `Storage.Snapshot()`。TinyKV 在 region worker 中生成快照和应用快照。当调用 `Snapshot()` 时，它实际上向 region worker 发送一个任务 `RegionTaskGen`。Region worker 的消息处理位于 `kv/raftstore/runner/region_task.go`。它扫描底层引擎以生成快照，并通过 channel 发送快照元数据。下次 Raft 调用 `Snapshot()` 时，它会检查快照生成是否完成。如果是，Raft 应该向其他 Peer 发送快照消息，快照的发送和接收工作由 `kv/storage/raft_storage/snap_runner.go` 处理。你不需要深入细节，只需要知道快照消息在接收后会由 `onRaftMsg` 处理。

然后快照将在下一次 Raft ready 中反映出来，所以你需要做的是修改 Raft ready 处理来处理快照的情况。当你确定要应用快照时，你可以更新 PeerStorage 的内存状态，如 `RaftLocalState`、`RaftApplyState` 和 `RegionLocalState`。同时，不要忘记将这些状态持久化到 kvdb 和 raftdb，并从 kvdb 和 raftdb 中删除过时的状态。此外，你还需要更新 `PeerStorage.snapState` 为 `snap.SnapState_Applying`，并通过 `PeerStorage.regionSched` 向 region worker 发送 `runner.RegionTaskApply` 任务并等待，直到 region worker 完成。

你应该运行 `make project2c` 来通过所有测试。
