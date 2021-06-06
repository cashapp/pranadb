package planner

import (
	"context"
	"github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/cascades"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/hint"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mock"
)

type planner struct {
	pushQueryOptimizer *cascades.Optimizer
	pullQueryOptimizer *cascades.Optimizer
}

func NewPlanner() planner {
	// TODO different rules for push and pull queries
	return planner{
		pushQueryOptimizer: cascades.NewOptimizer(),
		pullQueryOptimizer: cascades.NewOptimizer(),
	}
}

func (p *planner) CreateLogicalPlan(ctx context.Context, sessionContext sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (core.LogicalPlan, error) {

	hintProcessor := &hint.BlockHintProcessor{Ctx: sessionContext}

	builder, _ := core.NewPlanBuilder(sessionContext, is, hintProcessor)

	plan, err := builder.Build(ctx, node)
	if err != nil {
		return nil, err
	}

	logicalPlan, isLogicalPlan := plan.(core.LogicalPlan)
	if !isLogicalPlan {
		panic("Expected a logical plan")
	}

	return logicalPlan, nil
}

func (p * planner) CreatePhysicalPlan(ctx context.Context, sessionContext sessionctx.Context, logicalPlan core.LogicalPlan, isPushQuery, useCascades bool) (core.PhysicalPlan, error){
	if useCascades {
		// Use the new cost based optimizer
		if isPushQuery {
			physicalPlan, _, err := p.pushQueryOptimizer.FindBestPlan(sessionContext, logicalPlan)
			if err != nil {
				return nil, err
			}
			return physicalPlan, nil
		} else {
			physicalPlan, _, err := p.pullQueryOptimizer.FindBestPlan(sessionContext, logicalPlan)
			if err != nil {
				return nil, err
			}
			return physicalPlan, nil
		}
	} else {
		// Use the older optimizer
		physicalPlan, _, err := core.DoOptimize(ctx, sessionContext, 0, logicalPlan)
		if err != nil {
			return nil, err
		}
		return physicalPlan, nil
	}
}

func NewSessionContext() sessionctx.Context {
	sessCtx := mock.NewContext()
	kvClient := fakeKVClient{}
	storage := fakeStorage{client: kvClient}
	d := domain.NewDomain(storage, 0, 0, 0, nil)
	domain.BindDomain(sessCtx, d)
	sessCtx.Store = storage
	return sessCtx
}

type fakeKVClient struct {
}

func (f fakeKVClient) Send(ctx context.Context, req *kv.Request, vars interface{}, sessionMemTracker *memory.Tracker, enabledRateLimitAction bool) kv.Response {
	panic("should not be called")
}

func (f fakeKVClient) IsRequestTypeSupported(reqType, subType int64) bool {
	return true
}

// This is needed for the TiDB planner
type fakeStorage struct {
	client kv.Client
}

func (f fakeStorage) Begin() (kv.Transaction, error) {
	panic("implement me")
}

func (f fakeStorage) BeginWithOption(option tikv.StartTSOption) (kv.Transaction, error) {
	panic("implement me")
}

func (f fakeStorage) GetSnapshot(ver kv.Version) kv.Snapshot {
	panic("implement me")
}

func (f fakeStorage) GetClient() kv.Client {
	return f.client
}

func (f fakeStorage) GetMPPClient() kv.MPPClient {
	panic("implement me")
}

func (f fakeStorage) Close() error {
	panic("implement me")
}

func (f fakeStorage) UUID() string {
	panic("implement me")
}

func (f fakeStorage) CurrentVersion(txnScope string) (kv.Version, error) {
	panic("implement me")
}

func (f fakeStorage) GetOracle() oracle.Oracle {
	panic("implement me")
}

func (f fakeStorage) SupportDeleteRange() (supported bool) {
	panic("implement me")
}

func (f fakeStorage) Name() string {
	panic("implement me")
}

func (f fakeStorage) Describe() string {
	panic("implement me")
}

func (f fakeStorage) ShowStatus(ctx context.Context, key string) (interface{}, error) {
	panic("implement me")
}

func (f fakeStorage) GetMemCache() kv.MemManager {
	panic("implement me")
}

func (f fakeStorage) GetMinSafeTS(txnScope string) uint64 {
	panic("implement me")
}

func (f fakeStorage) GetLockWaits() ([]*deadlock.WaitForEntry, error) {
	panic("implement me")
}
