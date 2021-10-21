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

package executor

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

/***
 * Revoke Statement
 * See https://dev.mysql.com/doc/refman/5.7/en/revoke.html
 ************************************************************************************/
var (
	_ Executor = (*RevokeExec)(nil)
)

// RevokeExec executes RevokeStmt.
type RevokeExec struct {
	baseExecutor

	Privs      []*ast.PrivElem
	ObjectType ast.ObjectTypeType
	Level      *ast.GrantLevel
	Users      []*ast.UserSpec

	ctx  sessionctx.Context
	is   infoschema.InfoSchema
	done bool
}

// Next implements the Executor Next interface.
func (e *RevokeExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.done {
		return nil
	}
	e.done = true

	// Commit the old transaction, like DDL.
	if err := e.ctx.NewTxn(ctx); err != nil {
		return err
	}
	defer func() { e.ctx.GetSessionVars().SetInTxn(false) }()

	// Create internal session to start internal transaction.
	isCommit := false
	internalSession, err := e.getSysSession()
	if err != nil {
		return err
	}
	defer func() {
		if !isCommit {
			_, err := internalSession.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), "rollback")
			if err != nil {
				logutil.BgLogger().Error("rollback error occur at grant privilege", zap.Error(err))
			}
		}
		e.releaseSysSession(internalSession)
	}()

	_, err = internalSession.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), "begin")
	if err != nil {
		return err
	}

	sessVars := e.ctx.GetSessionVars()
	// Revoke for each user.
	for _, user := range e.Users {
		if user.User.CurrentUser {
			user.User.Username = sessVars.User.AuthUsername
			user.User.Hostname = sessVars.User.AuthHostname
		}

		// Check if user exists.
		exists, err := userExists(e.ctx, user.User.Username, user.User.Hostname)
		if err != nil {
			return err
		}
		if !exists {
			return errors.Errorf("Unknown user: %s", user.User)
		}
		err = e.checkDynamicPrivilegeUsage()
		if err != nil {
			return err
		}
		err = e.revokeOneUser(internalSession, user.User.Username, user.User.Hostname)
		if err != nil {
			return err
		}
	}

	_, err = internalSession.(sqlexec.SQLExecutor).ExecuteInternal(context.Background(), "commit")
	if err != nil {
		return err
	}
	isCommit = true
	domain.GetDomain(e.ctx).NotifyUpdatePrivilege(e.ctx)
	return nil
}

// Checks that dynamic privileges are only of global scope.
// Returns the mysql-correct error when not the case.
func (e *RevokeExec) checkDynamicPrivilegeUsage() error {
	var dynamicPrivs []string
	for _, priv := range e.Privs {
		if priv.Priv == mysql.ExtendedPriv {
			dynamicPrivs = append(dynamicPrivs, strings.ToUpper(priv.Name))
		}
	}
	if len(dynamicPrivs) > 0 && e.Level.Level != ast.GrantLevelGlobal {
		return ErrIllegalPrivilegeLevel.GenWithStackByArgs(strings.Join(dynamicPrivs, ","))
	}
	return nil
}

func (e *RevokeExec) revokeOneUser(internalSession sessionctx.Context, user, host string) error {
	dbName := e.Level.DBName
	if len(dbName) == 0 {
		dbName = e.ctx.GetSessionVars().CurrentDB
	}

	return nil
}

func (e *RevokeExec) revokePriv(internalSession sessionctx.Context, priv *ast.PrivElem, user, host string) error {
	switch e.Level.Level {
	case ast.GrantLevelGlobal:
		return e.revokeGlobalPriv(internalSession, priv, user, host)
	case ast.GrantLevelDB:
		return e.revokeDBPriv(internalSession, priv, user, host)
	case ast.GrantLevelTable:
		if len(priv.Cols) == 0 {
			return e.revokeTablePriv(internalSession, priv, user, host)
		}
		return e.revokeColumnPriv(internalSession, priv, user, host)
	}
	return errors.Errorf("Unknown revoke level: %#v", e.Level)
}

func (e *RevokeExec) revokeDynamicPriv(internalSession sessionctx.Context, privName string, user, host string) error {
	return nil
}

func (e *RevokeExec) revokeGlobalPriv(internalSession sessionctx.Context, priv *ast.PrivElem, user, host string) error {
	return nil
}

func (e *RevokeExec) revokeDBPriv(internalSession sessionctx.Context, priv *ast.PrivElem, userName, host string) error {
	return nil
}

func (e *RevokeExec) revokeTablePriv(internalSession sessionctx.Context, priv *ast.PrivElem, user, host string) error {
	return nil
}

func (e *RevokeExec) revokeColumnPriv(internalSession sessionctx.Context, priv *ast.PrivElem, user, host string) error {
	return nil
}

func privUpdateForRevoke(cur []string, priv mysql.PrivilegeType) ([]string, error) {
	p, ok := mysql.Priv2SetStr[priv]
	if !ok {
		return nil, errors.Errorf("Unknown priv: %v", priv)
	}
	cur = deleteFromSet(cur, p)
	return cur, nil
}
