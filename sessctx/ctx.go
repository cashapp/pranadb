package sessctx

import (
	"context"
	"fmt"
	"github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/sli"
	"github.com/pingcap/tipb/go-binlog"
)

func NewSessionContext(is infoschema.InfoSchema, pullQuery bool, database string) sessionctx.Context {
	kvClient := fakeKVClient{pullQuery: pullQuery}
	storage := fakeStorage{client: kvClient}
	d := domain.NewDomain(storage, 0, 0, 0, nil)

	sessVars := variable.NewSessionVars()
	// This is necessary to ensure prepared statement param markers are created properly in the
	// plan
	sessVars.StmtCtx.UseCache = true
	sessVars.StmtCtx.MemTracker = memory.NewTracker(0, -1)
	sessVars.CurrentDB = database

	ctx := sessCtx{
		is:          is,
		store:       storage,
		values:      make(map[fmt.Stringer]interface{}),
		sessionVars: sessVars,
	}
	domain.BindDomain(&ctx, d)
	return &ctx
}

func NewDummySessionContext() sessionctx.Context {
	return NewSessionContext(nil, false, "test")
}

type sessCtx struct {
	is          infoschema.InfoSchema
	store       kv.Storage
	sessionVars *variable.SessionVars
	values      map[fmt.Stringer]interface{}
	txn         dummyTxn
}

type dummyTxn struct {
	kv.Transaction
}

func (txn *dummyTxn) Valid() bool {
	return false
}

func (txn *dummyTxn) CacheTableInfo(id int64, info *model.TableInfo) {
	panic("should not be called")
}

func (txn *dummyTxn) GetTableInfo(id int64) *model.TableInfo {
	panic("should not be called")
}

func (s *sessCtx) NewTxn(ctx context.Context) error {
	panic("should not be called")
}

func (s *sessCtx) NewStaleTxnWithStartTS(ctx context.Context, startTS uint64) error {
	panic("should not be called")
}

func (s *sessCtx) Txn(active bool) (kv.Transaction, error) {
	return &s.txn, nil
}

func (s *sessCtx) GetClient() kv.Client {
	return s.store.GetClient()
}

func (s *sessCtx) GetMPPClient() kv.MPPClient {
	panic("should not be called")
}

func (s *sessCtx) SetValue(key fmt.Stringer, value interface{}) {
	s.values[key] = value
}

func (s sessCtx) Value(key fmt.Stringer) interface{} {
	value := s.values[key]
	return value
}

func (s sessCtx) ClearValue(key fmt.Stringer) {
	delete(s.values, key)
}

func (s sessCtx) GetInfoSchema() sessionctx.InfoschemaMetaVersion {
	return s.is
}

func (s sessCtx) GetSessionVars() *variable.SessionVars {
	return s.sessionVars
}

func (s sessCtx) GetSessionManager() util.SessionManager {
	panic("should not be called")
}

func (s sessCtx) RefreshTxnCtx(ctx context.Context) error {
	panic("should not be called")
}

func (s sessCtx) RefreshVars(ctx context.Context) error {
	panic("should not be called")
}

func (s sessCtx) InitTxnWithStartTS(startTS uint64) error {
	panic("should not be called")
}

func (s sessCtx) GetStore() kv.Storage {
	return s.store
}

func (s sessCtx) PreparedPlanCache() *kvcache.SimpleLRUCache {
	panic("should not be called")
}

func (s sessCtx) StoreQueryFeedback(feedback interface{}) {
	panic("should not be called")
}

func (s sessCtx) HasDirtyContent(tid int64) bool {
	return false
}

func (s sessCtx) StmtCommit() {
	panic("should not be called")
}

func (s sessCtx) StmtRollback() {
	panic("should not be called")
}

func (s sessCtx) StmtGetMutation(i int64) *binlog.TableMutation {
	panic("should not be called")
}

func (s sessCtx) DDLOwnerChecker() owner.DDLOwnerChecker {
	panic("should not be called")
}

func (s sessCtx) AddTableLock(infos []model.TableLockTpInfo) {
	panic("should not be called")
}

func (s sessCtx) ReleaseTableLocks(locks []model.TableLockTpInfo) {
	panic("should not be called")
}

func (s sessCtx) ReleaseTableLockByTableIDs(tableIDs []int64) {
	panic("should not be called")
}

func (s sessCtx) CheckTableLocked(tblID int64) (bool, model.TableLockType) {
	panic("should not be called")
}

func (s sessCtx) GetAllTableLocks() []model.TableLockTpInfo {
	panic("should not be called")
}

func (s sessCtx) ReleaseAllTableLocks() {
	panic("should not be called")
}

func (s sessCtx) HasLockedTables() bool {
	panic("should not be called")
}

func (s sessCtx) PrepareTSFuture(ctx context.Context) {
	panic("should not be called")
}

func (s sessCtx) StoreIndexUsage(tblID int64, idxID int64, rowsSelected int64) {
	panic("should not be called")
}

func (s sessCtx) GetTxnWriteThroughputSLI() *sli.TxnWriteThroughputSLI {
	panic("should not be called")
}

type fakeKVClient struct {
	pullQuery bool
}

func (f fakeKVClient) Send(ctx context.Context, req *kv.Request, vars interface{}, sessionMemTracker *memory.Tracker, enabledRateLimitAction bool) kv.Response {
	panic("should not be called")
}

func (f fakeKVClient) IsRequestTypeSupported(reqType, subType int64) bool {
	// By returning true we allow the optimiser to push select and aggregations to remote nodes
	// which is what we want for pull queries
	// But for push queries we don't want partial aggregations or pushing select to table scans
	// so we return false
	return f.pullQuery
}

// This is needed for the TiDB planner
type fakeStorage struct {
	client kv.Client
}

func (f fakeStorage) Begin() (kv.Transaction, error) {
	panic("should not be called")
}

func (f fakeStorage) BeginWithOption(option tikv.StartTSOption) (kv.Transaction, error) {
	panic("should not be called")
}

func (f fakeStorage) GetSnapshot(ver kv.Version) kv.Snapshot {
	panic("should not be called")
}

func (f fakeStorage) GetClient() kv.Client {
	return f.client
}

func (f fakeStorage) GetMPPClient() kv.MPPClient {
	panic("should not be called")
}

func (f fakeStorage) Close() error {
	panic("should not be called")
}

func (f fakeStorage) UUID() string {
	panic("should not be called")
}

func (f fakeStorage) CurrentVersion(txnScope string) (kv.Version, error) {
	panic("should not be called")
}

func (f fakeStorage) GetOracle() oracle.Oracle {
	panic("should not be called")
}

func (f fakeStorage) SupportDeleteRange() (supported bool) {
	panic("should not be called")
}

func (f fakeStorage) Name() string {
	panic("should not be called")
}

func (f fakeStorage) Describe() string {
	panic("should not be called")
}

func (f fakeStorage) ShowStatus(ctx context.Context, key string) (interface{}, error) {
	panic("should not be called")
}

func (f fakeStorage) GetMemCache() kv.MemManager {
	panic("should not be called")
}

func (f fakeStorage) GetMinSafeTS(txnScope string) uint64 {
	panic("should not be called")
}

func (f fakeStorage) GetLockWaits() ([]*deadlock.WaitForEntry, error) {
	panic("should not be called")
}
