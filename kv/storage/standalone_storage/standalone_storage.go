package standalone_storage

import (
	"path"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1)
	engines *engine_util.Engines
	config  *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
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

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).

	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engines.Close()
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
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

// 读取数据
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	kvTxn := s.engines.Kv.NewTransaction(false) // 创建一个新的 badger.Txn 对象，参数为 false 表示只读事务
	return NewStandAloneReader(kvTxn), nil
}

type StandAloneReader struct {
	kvTxn *badger.Txn
}

// 创建一个新的 StandAloneStorage 实例，传入一个 badger.Txn 对象作为参数
func NewStandAloneReader(kvTxn *badger.Txn) *StandAloneReader {
	return &StandAloneReader{
		kvTxn: kvTxn,
	}
}

func (r *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) { // 根据 cf 和 key 获取 value
	val, err := engine_util.GetCFFromTxn(r.kvTxn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *StandAloneReader) IterCF(cf string) engine_util.DBIterator { // 根据 cf 获取迭代器
	return engine_util.NewCFIterator(cf, r.kvTxn)
}

func (r *StandAloneReader) Close() { // 关闭 reader
	r.kvTxn.Discard()
}
