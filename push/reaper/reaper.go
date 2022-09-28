package reaper

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/cluster"
	"github.com/squareup/pranadb/common"
	"github.com/squareup/pranadb/metrics"
	"github.com/squareup/pranadb/table"
	"sync"
	"time"
)

var rowsReapedVec = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "pranadb_rows_reaped_total",
	Help: "counter for number of rows reaped, segmented by shard id",
}, []string{"shard_id"})

const minBatchDelay = 10 * time.Millisecond

type Reaper struct {
	lock               sync.Mutex
	started            bool
	store              cluster.Cluster
	shardID            uint64
	tableHeap          tableHeap
	runTimer           *time.Timer
	maxDeleteBatchSize int
	rowsReapedCounter  metrics.Counter
}

func NewReaper(store cluster.Cluster, maxDeleteBatchSize int, shardID uint64) *Reaper {
	sShardID := fmt.Sprintf("shard-%04d", shardID)
	rowsReapedCounter := rowsReapedVec.WithLabelValues(sShardID)
	return &Reaper{
		store:              store,
		maxDeleteBatchSize: maxDeleteBatchSize,
		shardID:            shardID,
		rowsReapedCounter:  rowsReapedCounter,
	}
}

func (r *Reaper) Start() {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.start(true)
}

func (r *Reaper) startNoSchedule() {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.start(false)
}

func (r *Reaper) start(schedule bool) {
	if r.started {
		return
	}
	r.started = true
	if schedule {
		// Add can occur before start so we need to schedule a check on start
		r.scheduleRunNoLock(0)
	}
}

func (r *Reaper) Stop() {
	r.lock.Lock()
	defer r.lock.Unlock()
	if !r.started {
		return
	}
	r.started = false
	if r.runTimer != nil {
		r.runTimer.Stop()
		r.runTimer = nil
	}
}

func (r *Reaper) AddTable(table *common.TableInfo) {
	if table.RetentionDuration < time.Second {
		panic("retention must be >= 1 second")
	}
	r.addTableNoSchedule(table)
	r.scheduleRunWithLock(0)
}

func (r *Reaper) addTableNoSchedule(table *common.TableInfo) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if !r.started {
		panic("not started")
	}
	// When a table is added CheckTime is zero - this will force the entry to the top of the stack and the reaper
	// will check if there are any actual rows, and if so, re-addto the stack with the correct CheckTime
	r.tableHeap.push(itemHolder{
		Table:     table,
		CheckTime: 0,
	})
}

func (r *Reaper) RemoveTable(table *common.TableInfo) {
	r.lock.Lock()
	defer r.lock.Unlock()
	elems := r.tableHeap.elems()
	r.tableHeap = tableHeap{}
	for _, item := range elems {
		if item.Table.ID == table.ID {
			continue
		}
		r.tableHeap.push(item)
	}
}

func (r *Reaper) run(schedule bool) (time.Duration, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	dur, err := r.doRun()
	r.runTimer = nil
	if err != nil {
		return 0, err
	}
	if schedule && dur != -1 {
		r.scheduleRunNoLock(dur)
	}
	return dur, nil
}

func (r *Reaper) doRun() (time.Duration, error) {
	if !r.started {
		return 0, nil
	}
	if r.tableHeap.len() == 0 {
		// -1 signifies don't reschedule
		return -1, nil
	}
	wb := cluster.NewWriteBatch(r.shardID)
	var oldest *itemHolder
	rowsDeleted := 0
	for rowsDeleted < r.maxDeleteBatchSize {
		oldest = r.tableHeap.peek()
		nowUnixMicros := time.Now().UnixMicro()
		if oldest != nil && nowUnixMicros-oldest.CheckTime >= 0 {
			r.tableHeap.pop()
			nextTime, numDeleted, err := r.processDeletes(r.shardID, oldest.Table, nowUnixMicros, wb, r.maxDeleteBatchSize-rowsDeleted)
			if err != nil {
				return 0, err
			}
			rowsDeleted += numDeleted
			r.tableHeap.push(itemHolder{
				CheckTime: nextTime,
				Table:     oldest.Table,
			})
		} else {
			break
		}
	}
	if rowsDeleted > 0 {
		if err := r.store.WriteBatchLocally(wb); err != nil {
			return 0, err
		}
		r.rowsReapedCounter.Add(float64(rowsDeleted))
		log.Debugf("reaper for shard %d deleted %d rows", r.shardID, rowsDeleted)
	}
	if rowsDeleted == r.maxDeleteBatchSize {
		log.Debug("no more rows returning zero")
		// There are more rows, schedule a batch immediately
		return 0, nil
	}
	delay := time.Duration(oldest.CheckTime-time.Now().UnixMicro()) * time.Microsecond
	if delay < minBatchDelay {
		// We have a min batch delay to avoid a busy loop
		delay = minBatchDelay
	}
	return delay, nil
}

func (r *Reaper) scheduleRunWithLock(delay time.Duration) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.scheduleRunNoLock(delay)
}

func (r *Reaper) scheduleRunNoLock(delay time.Duration) {
	if r.runTimer != nil {
		r.runTimer.Stop()
	}
	r.runTimer = time.AfterFunc(delay, func() {
		if _, err := r.run(true); err != nil {
			log.Errorf("failed to run reaper %+v", err)
		}
	})
}

// Process deletes for the table, returning time when next check for this table will occur
func (r *Reaper) processDeletes(shardID uint64, tab *common.TableInfo, nowUnixMicros int64, wb *cluster.WriteBatch, maxRows int) (int64, int, error) {
	keyStart := table.EncodeTableKeyPrefix(tab.RowTimeIndexID, shardID, 16)
	keyEnd := table.EncodeTableKeyPrefix(tab.RowTimeIndexID+1, shardID, 16)
	pkKeyBase := table.EncodeTableKeyPrefix(tab.ID, shardID, 16)

	retentionMicros := tab.RetentionDuration.Microseconds()

	iter := r.store.LocalIterator(keyStart, keyEnd)
	defer func() {
		if err := iter.Close(); err != nil {
			log.Errorf("failed to close local iterator %v", err)
		}
	}()
	rowCount := 0
	for iter.HasNext() && rowCount < maxRows {
		pair := iter.Next()

		// The index key is shard_id|index_id|1|row_time|pk
		// The last update time is unix millis
		rowTime, _, err := common.KeyDecodeTimestamp(pair.Key, 17, 6)
		if err != nil {
			return 0, 0, err
		}
		gt, err := rowTime.GoTime(time.UTC)
		if err != nil {
			return 0, 0, err
		}
		rowTimeUnixMicros := gt.UnixMicro()

		if nowUnixMicros-rowTimeUnixMicros < retentionMicros {
			// Not ready to delete
			return rowTimeUnixMicros + retentionMicros, rowCount, nil
		}

		pk := pair.Key[25:]
		pkKey := pkKeyBase
		pkKey = append(pkKey, pk...)
		wb.AddDelete(pkKey)
		// TODO use a delete range instead of deleting each one in turn
		wb.AddDelete(pair.Key)
		rowCount++
	}

	if rowCount == maxRows {
		// We have exceeded batch size but there are more rows, return 0 to ensure it's rescheduled next time at
		// the top of the heap
		return 0, rowCount, nil
	}

	// There are no more rows for the table so we schedule the next check in retention time from now
	return nowUnixMicros + retentionMicros, rowCount, nil
}
