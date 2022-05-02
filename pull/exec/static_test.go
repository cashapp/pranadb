package exec

import (
	"fmt"
	"testing"

	"github.com/squareup/pranadb/common"
)

func TestStaticRows_GetRows(t *testing.T) {
	wantRows := common.NewRows(
		[]common.ColumnType{common.IntColumnType},
		10,
	)
	for i := 0; i < 10; i++ {
		wantRows.AppendInt64ToColumn(0, int64(i))
	}
	for _, pageSize := range []int{1, 2, 3, 4, 5, 100} {
		t.Run(fmt.Sprintf("TestStaticRows_GetRows_pageSize=%d", pageSize), func(t *testing.T) {
			staticRows, err := NewStaticRows([]string{"index_column"}, wantRows)
			if err != nil {
				t.Errorf("NewStaticRows() error = %v", err)
				return
			}
			var page *common.Rows
			for i := 0; i < 100; i++ { // stop the loop eventually
				page, err = staticRows.GetRows(pageSize)
				if err != nil {
					t.Errorf("StaticRows.GetRows() error = %v", err)
					return
				}
				if page.RowCount() == 0 {
					break
				}
			}
			if page.RowCount() != 0 {
				t.Errorf("did not consume all rows")
			}
		})
	}
}
