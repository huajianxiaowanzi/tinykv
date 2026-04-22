// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	rand2 "math/rand"
	"sort"
	"time"

	"github.com/pingcap-incubator/tinykv/log"
	"golang.org/x/exp/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

func init() {
	rand.Seed(uint64(time.Now().UnixNano()))
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee  uint64
	transferElapsed int // 用于计时 transfer 的时间

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	randomElectionTimeout int
}

//r := newTestRaft(1, []uint64{1, 2, 3}, 10, 1, NewMemoryStorage())

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, conState, _ := c.Storage.InitialState() // 节点启动时从持久化存储中恢复之前保存的状态
	if c.peers == nil {                                // 新集群 c.peers != nil; 重启 c.peers == nill
		c.peers = conState.Nodes
	}
	rf := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              map[uint64]*Progress{},
		State:            StateFollower, // 刚启动只能是Follower
		votes:            map[uint64]bool{},
		msgs:             nil,
		Lead:             0,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
		leadTransferee:   0,
		PendingConfIndex: 0,
	}
	// 生成随机选举超时时间
	rf.resetRandomizedElectionTimeout()
	// 更新集群配置
	rf.Prs = make(map[uint64]*Progress)
	for _, id := range c.peers {
		rf.Prs[id] = &Progress{}
	}
	//3A
	// rf.PendingConfIndex = rf.initPendingConfIndex()

	return rf
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	prevLogIndex := r.Prs[to].Next - 1               // Prs[2].Next  表示 Leader 认为下一条要发送给节点 2 的日志索引
	prevLogTerm, err := r.RaftLog.Term(prevLogIndex) // 获取任期
	if err == nil {                                  // 日志在内存
		appendMsg := pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			From:    r.id,
			To:      to,
			Term:    r.Term,
			LogTerm: prevLogTerm,
			Index:   prevLogIndex,
			Entries: make([]*pb.Entry, 0),
			Commit:  r.RaftLog.committed,
		}
		// 期望覆盖或者追加到 follower 上的日志集合
		nextEntries := r.RaftLog.getEntries(prevLogIndex+1, 0)
		for i := range nextEntries {
			appendMsg.Entries = append(appendMsg.Entries, &nextEntries[i])
		}
		r.msgs = append(r.msgs, appendMsg)
		return true
	}
	// 获取任期 有错误，说明 nextIndex 存在于快照中，此时需要发送快照给 followers
	//2C
	r.sendSnapshot(to)
	log.Infof("[Snapshot Request]%d to %d, prevLogIndex %v, dummyIndex %v", r.id, to, prevLogIndex, r.RaftLog.dummyIndex)
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	})
}

func (r *Raft) sendSnapshot(to uint64) {
	snapshot, err := r.RaftLog.storage.Snapshot() // 从持久化存储中获取最近的快照
	if err != nil {
		// 生成 Snapshot 的工作是由 region worker 异步执行的，如果 Snapshot 还没有准备好
		// 此时会返回 ErrSnapshotTemporarilyUnavailable 错误，此时 leader 应该放弃本次 Snapshot Request
		// 等待下一次再请求 storage 获取 snapshot（通常来说会在下一次 heartbeat response 的时候发送 snapshot）
		return
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	})
	r.Prs[to].Next = snapshot.Metadata.Index + 1
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.followerTick()
	case StateCandidate:
		r.candidateTick()
	case StateLeader:
		r.leaderTick()
	}
}

func (r *Raft) followerTick() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		// MessageType_MsgHup 属于内部消息，也不需要经过 RawNode 处理
		r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup}) // 本地消息， 发起选举
	}
}

// Candidate 选举超时（没收到大多数投票）
func (r *Raft) candidateTick() {
	r.electionElapsed++
	// 选举超时（没收到大多数票）
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		// 重新发起选举
		// 注意：term 会 +1，这样能覆盖之前的"旧 Candidate"
		r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup})
	}
}

func (r *Raft) leaderTick() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgBeat})
	}

	//TODO 选举超时 判断心跳回应数量

	//TODO 3A 禅让机制
	if r.leadTransferee != None {
		// 在选举超时后领导权禅让仍然未完成，则 leader 应该终止领导权禅让，这样可以恢复客户端请求
		r.transferElapsed++
		if r.transferElapsed >= r.electionTimeout {
			r.leadTransferee = None
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if term > r.Term {
		//说明集群进入了新的任期,需要重置 Vote，允许在新任期内重新投票
		r.Vote = None
	}
	r.Term = term
	r.State = StateFollower
	r.Lead = lead
	r.electionElapsed = 0
	r.leadTransferee = None
	r.resetRandomizedElectionTimeout()

}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.votes[r.id] = true
	r.electionElapsed = 0
	r.resetRandomizedElectionTimeout()
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	// Progress 是 Leader 为每个 Follower 维护的日志复制进度.
	//Next 下一条要发送给该 Follower的日志索引 = LastIndex()+1
	//Match  已确认复制到该Follower的日志索引
	for id := range r.Prs {
		r.Prs[id].Next = r.RaftLog.LastIndex() + 1 // 初始化为 leader 的最后一条日志索引（后续出现冲突会往前移动）
		r.Prs[id].Match = 0                        // 还没开始复制
	}
	// Noop 日志复制到大多数节点 → committed
	// 之前任期的所有已复制日志也被隐式提交
	// 一旦 Leader 提交了 Noop，之前任期的日志就安全了，不会被后续 Leader 覆盖。
	//  TODO 没能理解
	r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
}

// resetRandomizedElectionTimeout 生成随机选举超时时间，范围在 [r.electionTimeout, 2*r.electionTimeout]
func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomElectionTimeout = r.electionTimeout + rand2.Intn(r.electionTimeout)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
// Step 处理的消息既有本地的（自己触发），也有网络的（其他节点发来）——所有状态变更都通过这个统一入口。 接受这些msg， step进行对应的处理
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.flowerStep(m) //可以收到 MsgHup、MsgRequestVote、MsgHeartBeat、MsgAppend
	case StateCandidate:
		r.candidateStep(m) //可以收到 MsgHup、MsgRequestVote、MsgHeartBeat、MsgRequestVoteResponse
	case StateLeader:
		r.leaderStep(m) //可以收到 MsgBeat、MsgHeartBeatResponse、MsgRequestVote
	}
	return nil
}

func (r *Raft) flowerStep(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// 成为候选者，开始发起投票
		r.handleStartElection(m)
	case pb.MessageType_MsgRequestVote:
		// 处理投票请求
		r.handleRequestVote(m)
	case pb.MessageType_MsgHeartbeat:
		// 接收心跳包，重置超时，称为跟随者，回发心跳包的resp
		r.handleHeartbeat(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		// follower 收到发来的禅让消息，转发给leader
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}

	case pb.MessageType_MsgTimeoutNow:
		// 收到Leader发来的要求立马开始选举的消息
		r.handleTimeoutNowRequest(m) // MessageType_MsgHup
	}

}

func (r *Raft) candidateStep(m pb.Message) {
	//Candidate 可以接收到的消息：
	//MsgHup、MsgRequestVote、MsgRequestVoteResponse、MsgHeartBeat
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// 成为候选者，开始发起投票
		r.handleStartElection(m)
	case pb.MessageType_MsgAppend:
		//Common Msg，用于 Leader 给其他节点同步日志条目// TODO 存在吗
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		//Common Msg，用于 Candidate 请求投票
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		//Common Msg，用于节点告诉 Candidate 投票结果
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgHeartbeat:
		// 接收心跳包，重置超时，称为跟随者，回发心跳包的resp
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgTransferLeader:
		//Local Msg，用于上层请求转移 Leader
		//要求领导转移其领导权
		// 非 leader 收到领导权禅让消息，需要转发给 leader
		if r.Lead != None {
			m.To = r.Lead
			r.msgs = append(r.msgs, m)
		}
	}
}

func (r *Raft) leaderStep(m pb.Message) {
	//Leader 可以接收到的消息：
	//MsgBeat、MsgHeartBeatResponse、MsgRequestVote、MsgPropose、MsgAppendResponse、MsgAppend
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		//Local Msg，用于告知 Leader 该发送心跳了，仅仅需要一个字段。
		r.broadcastHeartBeat()
	case pb.MessageType_MsgPropose:
		//Local Msg，用于上层请求 propose 条目
		r.handlePropose(m)
	case pb.MessageType_MsgAppend:
		//Common Msg，用于 Leader 给其他节点同步日志条目
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		//Common Msg，用于节点告诉 Leader 日志同步是否成功，和 MsgAppend 对应
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgRequestVote:
		//Common Msg，用于 Candidate 请求投票
		r.handleRequestVote(m)
	case pb.MessageType_MsgSnapshot:
		//Common Msg，用于 Leader 将快照发送给其他节点
		//3A
		//TODO project2C

	case pb.MessageType_MsgHeartbeat:
		//Common Msg，即 Leader 发送的心跳。
		//不同于论文中使用空的追加日志 RPC 代表心跳，TinyKV 给心跳一个单独的 MsgType
		//TODO Leader No processing required
	case pb.MessageType_MsgHeartbeatResponse:
		//Common Msg，即节点对心跳的回应
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgTransferLeader:
		// 3A
		//Local Msg，用于上层请求转移 Leader
		// leader 收到下台指令，进行处理
		r.handleTransferLeader(m)
	}
}

func (r *Raft) handleTransferLeader(m pb.Message) {
	// 判断消息发送者是否在集群中
	if _, ok := r.Prs[m.From]; !ok {
		return
	}
	// 如果消息发送者就是leader本身，则无事发生
	if m.From == r.id {
		return
	}
	// 判断是否有转让流程正在进行，如果是相同节点的转让流程就返回，否则的话终止上一个转让流程
	if r.leadTransferee != None {
		if r.leadTransferee == m.From { // 此前就有 这个禅让请求，所有停止第二个禅让。
			return
		}
		r.leadTransferee = None // 丢弃之前的 禅让请求
	}
	r.leadTransferee = m.From
	r.transferElapsed = 0
	if r.Prs[m.From].Match == r.RaftLog.LastIndex() { // 发送者的日志和leader相同， 直接禅让
		r.sendTimeoutNow(m.From)
	} else {
		r.sendAppend(m.From) // 同步新日志，Leader在接收到日志复制的response的时候继续禅让
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1}
		r.PendingConfIndex = None // 清除 PendingConfIndex 表示当前没有未完成的配置更新
	}
}

// removeNode remove a node from raft group
// 1.从集群中踢掉节点
// 2.检查降低的多数派门槛是否推进了
// 3.commit、标记配置变更完成。
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok {
		// 从 Prs 删除节点 → 集群成员减少
		delete(r.Prs, id)
		// 移除节点会降低多数派门槛，可能导致之前无法提交的日志现在可以提交了
		if r.State == StateLeader && r.maybeCommit() {
			log.Infof("[removeNode commit] %v leader commit new entry, commitIndex %v", r.id, r.RaftLog.committed)
			r.broadcastAppendEntry() // 广播更新所有 follower 的 commitIndex
		}
	}
	r.PendingConfIndex = None // 清除 PendingConfIndex 表示当前没有未完成的配置更新
}

/* ******************************** Msg Handle ******************************** */

// handleAppendEntries handle AppendEntries RPC request
// 1. m.term < r.term false
// 不包含  prevLogIndex    reply false
// 如果现有条目与新条目冲突，则删除现有条目，然后全部跟随该条目
// 附加日志中尚未添加的任何新条目
//   2. 基础响应（无优化）
//
//  appendEntryResp.Index = r.RaftLog.LastIndex()
//
//  基础做法：只告诉 Leader 自己的最后一条日志索引。
//
//  问题：Leader 只能逐次回退 Next，效率低。
//
//  无优化的同步过程：
//
//  Leader: [1:T1][2:T1][3:T2][4:T2][5:T2]
//  Follower: [1:T1][2:T1][6:T3][7:T3]
//
//  第 1 次：发送 prevLogIndex=5 → Follower 拒绝，LastIndex=7
//  第 2 次：发送 prevLogIndex=4 → Follower 拒绝，LastIndex=7
//  第 3 次：发送 prevLogIndex=3 → Follower 拒绝，LastIndex=7
//  第 4 次：发送 prevLogIndex=2 → 成功！从索引 3 开始追加
//
//  需要 4 轮 RPC！

// Leader 日志:
// 索引  [1]  [2]  [3]  [4]  [5]  [6]  [7]
// Term  [T1] [T1] [T2] [T2] [T2] [T3] [T3]
//
//	     ↑
//	prevLogIndex=5, prevLogTerm=T2
//
// Follower 日志:
// 索引  [1]  [2]  [3]  [4]  [5]
// Term  [T1] [T1] [T3] [T3] [T3]
//
//	↑    ↑
//	│    └─ 找到冲突 Term=T3 的第一条日志
//	└─ 返回 Index=2 (ent.Index - 1)
//
// 优化后的同步过程:
//
// 第 1 次：Leader 发送 prevLogIndex=5, Term=T2
//
//	↓
//	Follower 拒绝，响应 Index=2
//	(告诉 Leader: 从索引 2 之后开始发)
//	↓
//
// 第 2 次：Leader 发送 prevLogIndex=2, Term=T1
//
//	↓
//	成功！从索引 3 开始追加 [3:T2][4:T2][5:T2][6:T3][7:T3]
//
// 只需 2 轮 RPC！
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	appendEntryResp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
	}
	appendEntryResp.Reject = true
	if m.Term < r.Term {
		r.msgs = append(r.msgs, appendEntryResp)
		return
	}
	prevLogIndex := m.Index
	prevLogTerm := m.LogTerm
	r.becomeFollower(m.Term, m.From)

	if prevLogIndex > r.RaftLog.LastIndex() || r.RaftLog.TermNoErr(prevLogIndex) != prevLogTerm {
		//最后一条index和preLogIndex冲突、或者任期冲突
		//这时不能直接将 leader 传递过来的 Entries 覆盖到 follower 日志上
		//这里可以直接返回，以便让 leader 尝试 prevLogIndex - 1 这条日志
		//但是这样 follower 和 leader 之间的同步比较慢
		//TODO 日志冲突优化
		//	找到冲突任期的第一条日志，下次 leader 发送 AppendEntry 的时候会将 nextIndex 设置为 ConflictIndex
		// 	如果找不到的话就设置为 prevLogIndex 的前一个
		appendEntryResp.Index = r.RaftLog.LastIndex() //  // 用于提示 leader prevLogIndex 的开始位置是appendEntryResp.Index
		if prevLogIndex <= r.RaftLog.LastIndex() {
			conflictTerm := r.RaftLog.TermNoErr(prevLogIndex) // 获取冲突位置的 Term（Follower 在该索引的 Term）
			for _, ent := range r.RaftLog.entries {
				if ent.Term == conflictTerm {
					//找到冲突任期的上一个任期的idx位置
					appendEntryResp.Index = ent.Index - 1 // 这个 Index 告诉 Leader：你应该从 Index+1 的位置开始发送日志
					break
				}
			}
		}
	} else {
		//prevLogIndex没有冲突
		if len(m.Entries) > 0 {
			//3.
			idx, newLogIndex := m.Index+1, m.Index+1
			// 找到 follower 和 leader 在 new log 中出现冲突的位置
			// 这里是在上面的if break之后再次发来的同步
			for ; idx < r.RaftLog.LastIndex() && idx <= m.Entries[len(m.Entries)-1].Index; idx++ {
				term, _ := r.RaftLog.Term(idx)
				if term != m.Entries[idx-newLogIndex].Term { // 任期不同，冲突点
					break
				}
			}
			//被break处理了，发现冲突append logs和已有日志冲突
			if idx-newLogIndex != uint64(len(m.Entries)) {
				r.RaftLog.truncate(idx)                               // 截断冲突后面的所有日志
				r.RaftLog.appendNewEntry(m.Entries[idx-newLogIndex:]) // 并追加新的的日志
				r.RaftLog.stabled = min(r.RaftLog.stabled, idx-1)     // 更新持久化的日志索引
			}
		}
		// 更新 commitIndex
		if m.Commit > r.RaftLog.committed {
			// 取当前节点「已经和 leader 同步的日志」和 leader 「已经提交日志」索引的最小值作为节点的 commitIndex
			r.RaftLog.commit(min(m.Commit, m.Index+uint64(len(m.Entries))))
		}
		//同意
		appendEntryResp.Reject = false
		// 用于 leader 更新 NextIndex（存储的是下一次 AppendEntry 的 prevIndex）
		appendEntryResp.Index = m.Index + uint64(len(m.Entries))
		// 用于 leader 更新 committed
		appendEntryResp.LogTerm = r.RaftLog.TermNoErr(appendEntryResp.Index)
	}
	//回发
	r.msgs = append(r.msgs, appendEntryResp)
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Reject {
		if m.Term < r.Term {
			// 任期大于自己，变为Follower
			r.becomeFollower(m.Term, m.From)
		} else {
			// 否则就是因为 prevLog 日志冲突，继续尝试对 follower 同步日志
			r.Prs[m.From].Next = m.Index + 1
			r.sendAppend(m.From)
		}
		return
	}
	if r.Prs[m.From].maybeUpdate(m.Index) {
		// 由于有新的日志被复制了，因此有可能有新的日志可以提交执行，所以判断一下
		if r.maybeCommit() {
			// 广播更新所有 follower 的 commitIndex
			r.broadcastAppendEntry()
		}
	}
	// 3A
	// 由于follower 的日志不是最新的，所以leader没法禅让，leader给follower append日志，当leader收到 append的响应时， 继续禅让
	if r.leadTransferee == m.From && r.Prs[m.From].Match == r.RaftLog.LastIndex() {
		// AppendEntryResponse 回复来自 leadTransferee，检查日志是否是最新的
		// 如果 leadTransferee 达到了最新的日志则立即发起领导权禅让
		r.sendTimeoutNow(m.From)
	}

}

//	 T1: Leader 发送 Append(Entries=[5,6,7])  → 目标：节点 2
//	T2: Leader 发送 Append(Entries=[5,6,7,8,9]) → 目标：节点 2
//	T3: 消息 2 先到达 → 响应 Match=9
//	T4: 消息 1 后到达 → 响应 Match=7  ← 过期了！
//
// maybeUpdate 告诉leader进度是否真的更新了， n是Follower发过来的确认日志
func (pr *Progress) maybeUpdate(n uint64) bool {
	var updated bool
	// 判断是否是过期的消息回复
	if pr.Match < n {
		pr.Match = n
		pr.Next = pr.Match + 1
		updated = true
	}
	return updated
}

// Leader 判断是否有新日志可以被提交；一条日志被复制到大多数节点后，就可以被提交
func (r *Raft) maybeCommit() bool {
	matchArray := make(uint64Slice, 0)
	for _, progress := range r.Prs {
		matchArray = append(matchArray, progress.Match)
	}
	// 获取所有节点 match 的中位数，就是被大多数节点复制的日志索引
	sort.Sort(sort.Reverse(matchArray))     // 降序排列
	majority := len(r.Prs)/2 + 1            // // 大多数节点数量
	toCommitIndex := matchArray[majority-1] // 中位数
	//降序排序：[100, 98, 97, 95, 93]
	//↑
	//第 3 个 = 97
	//
	//含义：索引 97 及之前的日志已被至少 3 个节点复制 ✓

	// 检查是否可以提交 toCommitIndex
	return r.RaftLog.maybeCommit(toCommitIndex, r.Term)
}

func (r *Raft) broadcastAppendEntry() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

// 3A
func (r *Raft) sendTimeoutNow(to uint64) {
	// 发送任期超时消息， follower收到这个消息，立马开始选举
	r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgTimeoutNow, From: r.id, To: to})
}

// 3A
func (r *Raft) handleTimeoutNowRequest(m pb.Message) {
	if _, ok := r.Prs[r.id]; !ok {
		return
	}
	// 直接发起选举
	if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup}); err != nil {
		log.Panic(err)
	}
}

// broadcastHeartBeat 广播心跳消息
func (r *Raft) broadcastHeartBeat() {
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id)
	}
	r.heartbeatElapsed = 0
}

// handleHeartbeat handle Heartbeat RPC request
// 接收心跳包，重置超时，称为跟随者，回发心跳包的resp
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	heartBeatResp := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	if r.Term > m.Term { // 比leader的term还大， leader收到后要变成follower
		heartBeatResp.Reject = true
	} else {
		r.becomeFollower(m.Term, m.From)
	}
	r.msgs = append(r.msgs, heartBeatResp)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Reject {
		r.becomeFollower(m.Term, m.From)
	} else {
		// 心跳同步成功
		// 检查该节点的日志是不是和自己是同步的，由于有些节点断开连接并又恢复了链接
		// 因此 leader 需要及时向这些节点同步日志
		if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}

	}
}

// handleSnapshot handle Snapshot RPC request leader和follower都要从snapshot中恢复数据
// 从 SnapshotMetadata 中恢复 Raft 的内部状态，例如 term、commit、membership information
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	resp := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		Term:    r.Term,
	}
	meta := m.Snapshot.Metadata
	// 1. 如果 term 小于自身的 term 直接拒绝这次快照的数据
	if m.Term < r.Term {
		resp.Reject = true
	} else if r.RaftLog.committed >= meta.Index {
		// 2. 如果已经提交的日志大于等于快照中的日志，也需要拒绝这次快照
		// 因为 commit 的日志必定会被 apply，如果被快照中的日志覆盖的话就会破坏一致性
		resp.Reject = true
		resp.Index = r.RaftLog.committed
	} else {
		// 3. 需要安装日志
		r.becomeFollower(m.Term, m.From)
		// 更新日志数据
		r.RaftLog.dummyIndex = meta.Index + 1
		r.RaftLog.committed = meta.Index
		r.RaftLog.applied = meta.Index
		r.RaftLog.stabled = meta.Index
		r.RaftLog.pendingSnapshot = m.Snapshot // 正在接收快照
		r.RaftLog.entries = make([]pb.Entry, 0)
		// 更新集群配置
		r.Prs = make(map[uint64]*Progress)
		for _, id := range meta.ConfState.Nodes {
			r.Prs[id] = &Progress{Next: r.RaftLog.LastIndex() + 1}
		}
		// 更新 response，提示 leader 更新 nextIndex
		resp.Index = meta.Index
	}
	r.msgs = append(r.msgs, resp)
}

// 成为候选者，开始发起投票
func (r *Raft) handleStartElection(m pb.Message) {
	r.becomeCandidate()
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	//发起 RequestVote 请求
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{ // 拉票请求
			MsgType: pb.MessageType_MsgRequestVote,
			To:      id,
			From:    r.id,
			Term:    r.Term,
			LogTerm: r.RaftLog.LastTerm(),
			Index:   r.RaftLog.LastIndex(),
		})
	}
	r.votes = make(map[uint64]bool) // 重置 votes
	r.votes[r.id] = true            // 开始的时候只有自己投票给自己
}

func (r *Raft) handleRequestVote(m pb.Message) {
	voteRep := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Reject:  false,
	}
	//1. 判断 Msg 的 Term 是否大于等于自己的 Term，是则更新
	if m.Term > r.Term {
		// term比自己大 变成follower
		r.becomeFollower(m.Term, None)
	}
	if (m.Term > r.Term || m.Term == r.Term && (r.Vote == None || r.Vote == m.From)) && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
		// 投票
		// 1. Candidate 任期大于自己并且日志足够新
		// 2. Candidate 任期和自己相等并且自己在当前任期内没有投过票或者已经投给了 Candidate，并且 Candidate 的日志足够新
		r.becomeFollower(m.Term, None)
		r.Vote = m.From
	} else {
		// 拒绝投票
		// 1. Candidate 的任期小于自己
		// 2. 自己在当前任期已经投过票了
		// 3. Candidate 的日志不够新
		voteRep.Reject = true
	}
	r.msgs = append(r.msgs, voteRep)
}

// handleRequestVoteResponse 候选者节点收到 RequestVote Response 时候的处理
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	r.votes[m.From] = !m.Reject
	// 更新节点的投票信息
	count := 0
	for _, agree := range r.votes {
		if agree {
			count++
		}
	}
	majority := len(r.Prs)/2 + 1

	if m.Reject {
		if r.Term < m.Term {
			// 如果某个节点拒绝了 RequestVote 请求，并且任期大于自身的任期
			// 那么 Candidate 回到 Follower 状态
			r.becomeFollower(m.Term, None)
		}
		if len(r.votes)-count >= majority {
			// 半数以上节点拒绝投票给自己，此时需要变回 follower
			r.becomeFollower(m.Term, None)
		}
	} else {
		if count >= majority {
			r.becomeLeader()
		}
	}
}

// handlePropose 追加从上层应用接收到的新日志，并广播给 follower
func (r *Raft) handlePropose(m pb.Message) {
	r.appendEntry(m.Entries)
	// leader 处于领导权禅让，停止接收新的请求
	if r.leadTransferee != None {
		return
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	if len(r.Prs) == 1 {
		r.RaftLog.commit(r.RaftLog.LastIndex())
	} else {
		r.broadcastAppendEntry()
	}
}

func (r *Raft) appendEntry(entries []*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex() // leader 最后一条日志的索引
	for i := range entries {
		// 设置新日志的索引和任期
		entries[i].Index = lastIndex + uint64(i) + 1
		entries[i].Term = r.Term
		if entries[i].EntryType == pb.EntryType_EntryConfChange {
			r.PendingConfIndex = entries[i].Index
		}
	}
	r.RaftLog.appendNewEntry(entries)
}
