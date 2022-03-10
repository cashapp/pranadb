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
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/errors"
	"os"
	"sync"
)

func NewInjector() Injector {
	return &defaultInjector{}
}

func NewDummyInjector() Injector {
	return &dummyInjector{}
}

type Injector interface {
	RegisterFailpoint(name string, exit bool, err error) (Failpoint, error)
	GetFailpoint(name string) (Failpoint, error)
	Start() error
	Stop() error
}

type Failpoint interface {
	Check() error
	SetActive(active bool)
}

type defaultInjector struct {
	failpoints sync.Map
	lock       sync.Mutex
}

type defaultFailpoint struct {
	name   string
	error  error
	exit   bool
	active common.AtomicBool
}

func (i *defaultInjector) RegisterFailpoint(name string, exit bool, err error) (Failpoint, error) {
	i.lock.Lock()
	defer i.lock.Unlock()
	o, ok := i.failpoints.Load(name)
	if ok {
		fp, ok := o.(Failpoint)
		if !ok {
			panic("not a Failpoint")
		}
		return fp, nil
	}
	if !exit && err == nil {
		return nil, errors.Error("if Failpoint is not Exit=true then an error must be specified")
	}
	fp := &defaultFailpoint{
		name:  name,
		error: err,
		exit:  exit,
	}
	i.failpoints.Store(name, fp)
	return fp, nil
}

func (i *defaultInjector) GetFailpoint(name string) (Failpoint, error) {
	o, ok := i.failpoints.Load(name)
	if !ok {
		return nil, errors.Errorf("no failpoint registered with name %s", name)
	}
	fp, ok := o.(Failpoint)
	if !ok {
		panic("not a Failpoint")
	}
	return fp, nil
}

func (f *defaultFailpoint) Check() error {
	if !f.active.Get() {
		return nil
	}
	if f.exit {
		log.Trace("Exiting as failpoint %s has been triggered", f.name)
		os.Exit(1)
		return nil
	}
	return f.error
}

func (f *defaultFailpoint) SetActive(active bool) {
	f.active.Set(active)
}

func (i *defaultInjector) Start() error {
	return nil
}

func (i *defaultInjector) Stop() error {
	return nil
}

type dummyInjector struct {
}

func (d *dummyInjector) RegisterFailpoint(name string, exit bool, err error) (Failpoint, error) {
	return &dummyFailpoint{}, nil
}

func (i *dummyInjector) GetFailpoint(name string) (Failpoint, error) {
	return &dummyFailpoint{}, nil
}

func (i *dummyInjector) Start() error {
	return nil
}

func (i *dummyInjector) Stop() error {
	return nil
}

type dummyFailpoint struct {
}

func (df *dummyFailpoint) Check() error {
	return nil
}

func (f *dummyFailpoint) SetActive(active bool) {
}
