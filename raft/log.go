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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.自从上次快照以来，所有的持久化条目
	storage Storage

	// committed is the highest log position that is known to be in节点认为已经提交的日志 Index；
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed。节点最新应用的日志 Index；
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.节点已经持久化的最后一条日志的 Index
	stabled uint64

	// all entries that have not yet compact. 所有数据包括持久化的和为持久化的
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C) 待处理的快照
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	//用于节点记录上一条追加的日志 Index
	//在 follower 更新自己的 committed 时
	//需要把leader 传来的 committed 和其进行比较
	dummyIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
// newLog使用给定存储返回日志。它将日志恢复到刚刚提交并应用最新快照的状态。
//例子：
//- firstIndex = 100 (snapshot 包含 1-99 条日志)
//- applied = 99 = 100 - 1
//- 表示：snapshot 之前的已经应用了，从 100 开始还没应用

// lastIndex > Commit
//[1, 2, 3, 4, 5, 6, 7]
//↑    ↑
//Commit  lastIndex
//已提交=5  总长=7
//原因：日志 6、7 还没被大多数节点确认

// Leader 追加日志 → 先写磁盘 → 再发送给 Follower → 大多数确认后标记为 committed
//
//	即使崩溃，日志也在磁盘上, 所以未commit的日志也会持久化到磁盘中
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, _ := storage.FirstIndex() // 第一条日志索引（snapshot 之后）
	lastIndex, _ := storage.LastIndex()   // 最后一条日志索引
	entries, _ := storage.Entries(firstIndex, lastIndex+1)
	hardState, _, _ := storage.InitialState() // 持久化的集群状态（term/vote/commit）

	rl := &RaftLog{
		storage:   storage,
		committed: hardState.Commit,
		applied:   firstIndex - 1, // 刚启动时，状态机还没执行任何日志
		stabled:   lastIndex,      // 所有从磁盘读取的日志都是已稳定的
		entries:   entries,

		pendingSnapshot: nil,
		dummyIndex:      firstIndex,
	}
	return rl
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
// 日志压缩 = 把已经持久化且已应用的旧日志扔掉，只保留最近的，防止内存无限增长。
// 在 storage 压缩后同步更新内存 entries，丢弃已经不存在的旧日志，让内存和磁盘保持一致。
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	newFirst, _ := l.storage.FirstIndex() // 之前的都是快照了的，可以丢弃。只保留后续的日志即可
	if newFirst > l.dummyIndex {
		entries := l.entries[newFirst-l.dummyIndex:]
		l.entries = make([]pb.Entry, 0)
		l.entries = append(l.entries, entries...)
	}
	l.dummyIndex = newFirst // TODO
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
// allEntries返回所有未压缩的条目
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.LastIndex() == l.stabled {
		return make([]pb.Entry, 0)
	}
	return l.getEntries(l.stabled+1, 0)
}

// getEntries 返回 [start, end) 之间的所有日志，end = 0 表示返回 start 开始的所有日志
func (l *RaftLog) getEntries(start, end uint64) []pb.Entry {
	if end == 0 {
		end = l.LastIndex() + 1
	}
	// 关键：l.entries 是 0 基数组，但日志索引从 dummyIndex 开始
	// 需要转换：数组索引 = 日志索引 - dummyIndex
	start, end = start-l.dummyIndex, end-l.dummyIndex
	return l.entries[start:end]
}

// nextEnts returns all the committed but not applied entries
// 返回在(applied，committed]之间的日志
// 返回所有已经提交但没有应用的日志
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	diff := l.dummyIndex - 1
	// 只有 committed > applied 时，才有待应用的日志， 当等于的时候说明都apply了
	if l.committed > l.applied {
		return l.entries[l.applied-diff : l.committed-diff]
	}
	return make([]pb.Entry, 0)
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.dummyIndex - 1 + uint64(len(l.entries))
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i >= l.dummyIndex {
		return l.entries[i-l.dummyIndex].Term, nil
	}
	// 2. 判断 i 是否等于当前正准备安装的快照的最后一条日志
	if !IsEmptySnap(l.pendingSnapshot) && i == l.pendingSnapshot.Metadata.Index {
		return l.pendingSnapshot.Metadata.Term, nil
	}

	// 3. 否则的话 i 只能是快照中的日志
	term, err := l.storage.Term(i)
	return term, err
}

func (l *RaftLog) TermNoErr(i uint64) uint64 {
	if i >= l.dummyIndex {
		return l.entries[i-l.dummyIndex].Term
	}
	//有待处理的快照 && 查询的是快照的最后一条索引
	if !IsEmptySnap(l.pendingSnapshot) && i == l.pendingSnapshot.Metadata.Index {
		return l.pendingSnapshot.Metadata.Term
	}
	// 索引 < dummyIndex（日志已持久化但不在内存中）
	term, _ := l.storage.Term(i)
	return term
}

// 截断后续的日志
func (l *RaftLog) truncate(startIndex uint64) {
	if len(l.entries) > 0 {
		l.entries = l.entries[:startIndex-l.dummyIndex]
	}
}

// appendEntry 添加新的日志，并返回最后一条日志的索引
func (l *RaftLog) appendNewEntry(ents []*pb.Entry) uint64 {
	for i := range ents {
		l.entries = append(l.entries, *ents[i])
	}
	return l.LastIndex()
}

func (l *RaftLog) commit(toCommit uint64) {
	l.committed = toCommit
}

// LastTerm 返回最后一条日志的索引
func (l *RaftLog) LastTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	lastIndex := l.LastIndex() - l.dummyIndex
	return l.entries[lastIndex].Term
}

// 选举限制
func (l *RaftLog) isUpToDate(index, term uint64) bool {
	//  1. 比较最后一条日志的 Term，Term 大的更新
	//  2. 如果 Term 相同，Index 大的更新（日志更长）
	return term > l.LastTerm() || (term == l.LastTerm() && index >= l.LastIndex())
}

// maybeCommit 检查一个被大多数节点复制的日志是否需要提交
func (l *RaftLog) maybeCommit(toCommit, term uint64) bool {
	commitTerm, _ := l.Term(toCommit)
	if toCommit > l.committed && commitTerm == term {
		// 只有当新的索引大于当前 committed 时，才有提交的必要。
		// 要提交的日志必须是当前任期创建的
		l.commit(toCommit)
		return true
	}
	return false
}
