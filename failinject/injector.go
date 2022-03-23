/*
 *  Copyright 2022 Square Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package failinject

import (
	"fmt"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"sync"
)

func NewInjector() Injector {
	return &defaultInjector{failpoints: make(map[string]*defaultFailpoint)}
}

type Injector interface {
	RegisterFailpoint(name string) (Failpoint, error)
	GetFailpoint(name string) Failpoint
	Start() error
	Stop() error
}

type Failpoint interface {
	CheckFail() error
	SetFailAction(action FailAction)
	Deactivate()
}

type FailAction func() error

type defaultInjector struct {
	failpoints map[string]*defaultFailpoint
	lock       sync.Mutex
}

type defaultFailpoint struct {
	name       string
	active     common.AtomicBool
	failAction FailAction
}

func (i *defaultInjector) RegisterFailpoint(name string) (Failpoint, error) {
	i.lock.Lock()
	defer i.lock.Unlock()
	fp := &defaultFailpoint{
		name: name,
	}
	i.failpoints[name] = fp
	return fp, nil
}

func (i *defaultInjector) GetFailpoint(name string) Failpoint {
	i.lock.Lock()
	defer i.lock.Unlock()
	fp, ok := i.failpoints[name]
	if !ok {
		panic(fmt.Sprintf("no failpoint registered with name %s", name))
	}
	return fp
}

func (f *defaultFailpoint) CheckFail() error {
	if !f.active.Get() {
		return nil
	}
	if f.failAction == nil {
		return errors.Errorf("no fail action specfied for failpoint %s", f.name)
	}
	return f.failAction()
}

func (f *defaultFailpoint) SetFailAction(action FailAction) {
	f.active.Set(true)
	f.failAction = action
}

func (f *defaultFailpoint) Deactivate() {
	f.active.Set(false)
	f.failAction = nil
}

func (i *defaultInjector) Start() error {
	return i.registerFailpoints()
}

func (i *defaultInjector) Stop() error {
	return nil
}

func (i *defaultInjector) registerFailpoints() error {
	_, err := i.RegisterFailpoint("create_mv_1")
	if err != nil {
		return err
	}
	_, err = i.RegisterFailpoint("create_mv_2")
	if err != nil {
		return err
	}
	_, err = i.RegisterFailpoint("fill_to_1")
	return err
}
