// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}
func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

type storeSlice []*core.StoreInfo

func (a storeSlice) Len() int           { return len(a) }
func (a storeSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a storeSlice) Less(i, j int) bool { return a[i].GetRegionSize() < a[j].GetRegionSize() }

// Schedule 避免太多 region 堆积在一个 store
func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	// 1. 选出所有的 suitableStores
	stores := make(storeSlice, 0)
	for _, store := range cluster.GetStores() { // 所有store
		// 适合被移动的 store 需要满足停机时间不超过 MaxStoreDownTime
		if store.IsUp() && store.DownTime() < cluster.GetMaxStoreDownTime() { // Store 在线 && 停机时间没超过阈值
			stores = append(stores, store)
		}
	}
	if len(stores) < 2 { // 至少需要 2 个 Store 才能做均衡。只有 1 个 Store 的话，没有目标可以搬
		return nil
	}
	// 2. 遍历 suitableStores，找到要移动的 region 和 store。 首先找Pending Region, 其次找follower，最后找 leader
	sort.Sort(stores) // 根据store 占用的数据字节大小排序， 均衡的目标是磁盘空间
	var fromStore, toStore *core.StoreInfo
	var region *core.RegionInfo
	for i := len(stores) - 1; i >= 0; i-- { // 从数据字节 最大的store 开始遍历
		var regions core.RegionsContainer
		// 正在迁移中但还没完成的 Region
		cluster.GetPendingRegionsWithLock(stores[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		// 随机挑一个
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			fromStore = stores[i]
			break
		}
		// 该 Store 上作为 Follower 的 Region
		cluster.GetFollowersWithLock(stores[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			fromStore = stores[i]
			break
		}
		// 该 Store 上作为 Leader 的 Region
		cluster.GetLeadersWithLock(stores[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			fromStore = stores[i]
			break
		}
	}
	// 如果遍历完所有 Store 都没找到可搬的 Region，返回 nil，本次不调度。
	if region == nil {
		return nil
	}
	// 3. 判断目标 region 的 store 数量，如果小于 cluster.GetMaxReplicas 直接放弃本次操作
	storeIds := region.GetStoreIds() // 返回这个region 的所有副本所在的store ID 集合
	if len(storeIds) < cluster.GetMaxReplicas() {
		// 当前的region的节点数少于规定， 说明有store 挂了或者正在迁移中，这时候做均衡 没有意义——先把副本补齐比搬来搬去更重要。你总不能在只有 2
		// 个副本的时候还把其中一个搬走，那样风险更大。
		return nil
	}
	// 4. 再次从 suitableStores 里面找到一个目标 store，目标 store 不能在原来的 region 里面
	for i := 0; i < len(stores); i++ { // 找到节点最少的store
		if _, ok := storeIds[stores[i].GetID()]; !ok { // 找一个没有该region的节点的store，就搬到这个store里来
			toStore = stores[i]
			break
		}
	}
	if toStore == nil {
		return nil
	}
	// 5. fromStore: 50MB    toStore: 45MB
	//    差值 = 50 - 45 = 5MB
	//    5MB < 10MB → 放弃！ 两个store 数据均衡， 没有搬迁节点的必要
	if fromStore.GetRegionSize()-toStore.GetRegionSize() < region.GetApproximateSize() {
		return nil
	}
	// 6. 创建 CreateMovePeerOperator 操作并返回
	newPeer, _ := cluster.AllocPeer(toStore.GetID()) // 目标store创建一个peer
	desc := fmt.Sprintf("move-from-%d-to-%d", fromStore.GetID(), toStore.GetID())
	op, _ := operator.CreateMovePeerOperator(desc, cluster, region, operator.OpBalance, fromStore.GetID(), toStore.GetID(), newPeer.GetId())
	return op
}
