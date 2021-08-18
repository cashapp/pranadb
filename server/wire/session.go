package wire

import (
	"context"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"

	"github.com/squareup/pranadb/sess"
)

type sessionKeyType int

var (
	sessionListenerKey = sessionKeyType(679853)
	sessionConnKey     = sessionKeyType(324579)
)

// SetSession associated with the current client connection.
func SetSession(ctx context.Context, logger *zap.Logger, session *sess.Session) {
	sm := sessionManagerFromContext(ctx)
	sessionKey := sessionKeyFromContext(ctx)
	logger.Info("Created session", zap.String("sessionKey", sessionKey))
	sm.sessions.Store(sessionKey, session)
}

// SessionFromContext retrieves the sess.Session associated with ctx, or nil.
func SessionFromContext(ctx context.Context) *sess.Session {
	sm := sessionManagerFromContext(ctx)
	sessionKey := sessionKeyFromContext(ctx)
	session, ok := sm.sessions.Load(sessionKey)
	if !ok {
		return nil
	}
	return session.(*sess.Session)
}

type sessionManager struct {
	sessions sync.Map
	logger   *zap.Logger
}

// RegisterSessionManager is passed to grpc.NewServer() to register the SessionManager.
func RegisterSessionManager(logger *zap.Logger) grpc.ServerOption {
	return grpc.StatsHandler(&sessionManager{logger: logger})
}

var _ stats.Handler = &sessionManager{}

func (s *sessionManager) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return ctx
}

func (s *sessionManager) HandleRPC(ctx context.Context, rpcStats stats.RPCStats) {
}

func (s *sessionManager) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	sessionKey := info.RemoteAddr.String()
	ctx = context.WithValue(ctx, sessionConnKey, sessionKey)
	ctx = context.WithValue(ctx, sessionListenerKey, s)
	s.logger.Info("Created session", zap.String("sessionKey", sessionKey))
	return ctx
}

func (s *sessionManager) HandleConn(ctx context.Context, connStats stats.ConnStats) {
	if _, ok := connStats.(*stats.ConnEnd); ok {
		sessionKey := sessionKeyFromContext(ctx)
		s.logger.Info("Closed session", zap.String("sessionKey", sessionKey))
		s.sessions.Delete(sessionKey)
	}
}

func sessionKeyFromContext(ctx context.Context) string {
	peer := ctx.Value(sessionConnKey)
	if peer == nil {
		panic("couldn't extract gRPC peer from context")
	}
	return peer.(string)
}

func sessionManagerFromContext(ctx context.Context) *sessionManager {
	value := ctx.Value(sessionListenerKey)
	if value == nil {
		panic("context does not contain session manager")
	}
	return value.(*sessionManager)
}
