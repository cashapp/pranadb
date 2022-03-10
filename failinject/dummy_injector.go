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

func NewDummyInjector() Injector {
	return &dummyInjector{}
}

type dummyInjector struct {
}

func (d *dummyInjector) RegisterFailpoint(name string, exit bool, err error) (Failpoint, error) {
	return &dummyFailpoint{}, nil
}

func (d *dummyInjector) GetFailpoint(name string) Failpoint {
	return &dummyFailpoint{}
}

func (d *dummyInjector) Start() error {
	return nil
}

func (d *dummyInjector) Stop() error {
	return nil
}

type dummyFailpoint struct {
}

func (df *dummyFailpoint) Check() error {
	return nil
}

func (df *dummyFailpoint) SetActive(active bool) {
}
