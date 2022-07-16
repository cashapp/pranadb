package interruptor

import (
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/common"
	"sync"
	"time"
)

var td InterruptManager = &interruptManager{}
var tdLock = sync.Mutex{}

type InterruptManager interface {
	MaybeInterrupt(interruptName string, interruptor *Interruptor) bool
}

func GetInterruptManager() InterruptManager {
	tdLock.Lock()
	defer tdLock.Unlock()
	return td
}

func ActivateTestInterruptManager() {
	tdLock.Lock()
	defer tdLock.Unlock()
	td = &DelayingInterruptManager{}
}

type interruptManager struct {
}

func (d *interruptManager) MaybeInterrupt(interruptName string, interruptor *Interruptor) bool {
	return interruptor.Interrupted.Get()
}

// DelayingInterruptManager is an InterruptManager which delays the goroutine until the interrupt occurs - used in testing
type DelayingInterruptManager struct {
	activeInterrupts sync.Map
}

func (d *DelayingInterruptManager) MaybeInterrupt(interruptName string, interruptor *Interruptor) bool {
	_, ok := d.activeInterrupts.Load(interruptName)
	if !ok {
		return interruptor.Interrupted.Get()
	}
	start := time.Now()
	for {
		if interruptor.Interrupted.Get() {
			return true
		}
		time.Sleep(1 * time.Millisecond)
		if time.Now().Sub(start) > 10*time.Second {
			log.Error("time out in DelayingInterruptManager")
			break
		}
	}
	return false
}

func (d *DelayingInterruptManager) ActivateDelay(interruptName string) {
	d.activeInterrupts.Store(interruptName, struct{}{})
}

func (d *DelayingInterruptManager) DeactivateDelay(interruptName string) {
	d.activeInterrupts.Delete(interruptName)
}

type Interruptor struct {
	Interrupted common.AtomicBool
}

func (i *Interruptor) Interrupt() {
	i.Interrupted.Set(true)
}
