package exec

import (
	"testing"

	"github.com/squareup/pranadb/common"
)

func TestPullLimit_GetRows(t *testing.T) {
	colTypes := []common.ColumnType{common.BigIntColumnType}
	rowsFactory := common.NewRowsFactory(colTypes)
	existingRows := rowsFactory.NewRows(10)
	for i := 0; i < 10; i++ {
		existingRows.AppendInt64ToColumn(0, int64(i))
	}
	makeBase := func() pullExecutorBase {
		base := pullExecutorBase{
			colNames:    []string{"test_col"},
			colTypes:    colTypes,
			rowsFactory: rowsFactory,
		}
		staticRows, _ := NewStaticRows(base.colNames, existingRows)
		base.AddChild(staticRows)
		return base
	}

	type fields struct {
		pullExecutorBase pullExecutorBase
		count            uint64
		offset           uint64
		rows             *common.Rows
		cursor           int
	}

	type args struct {
		maxRowsToReturn int
	}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []int64
		wantErr bool
	}{
		{
			name: "non-zero offset",
			fields: fields{
				pullExecutorBase: makeBase(),
				count:            10,
				offset:           1,
			},
			args: args{
				maxRowsToReturn: 100,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "over limit",
			fields: fields{
				pullExecutorBase: makeBase(),
				count:            999_999_999,
			},
			args: args{
				maxRowsToReturn: 100,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "limit 0",
			fields: fields{
				pullExecutorBase: makeBase(),
				count:            0,
			},
			args: args{
				maxRowsToReturn: 100,
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "limit 1",
			fields: fields{
				pullExecutorBase: makeBase(),
				count:            1,
			},
			args: args{
				maxRowsToReturn: 100,
			},
			want:    []int64{0},
			wantErr: false,
		},
		{
			name: "limit 100",
			fields: fields{
				pullExecutorBase: makeBase(),
				count:            100,
			},
			args: args{
				maxRowsToReturn: 100,
			},
			want:    []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
			wantErr: false,
		},
		{
			name: "limit 100 max rows 5",
			fields: fields{
				pullExecutorBase: makeBase(),
				count:            100,
			},
			args: args{
				maxRowsToReturn: 5,
			},
			want:    []int64{0, 1, 2, 3, 4},
			wantErr: false,
		},
		{
			name: "limit 5 max rows 5",
			fields: fields{
				pullExecutorBase: makeBase(),
				count:            5,
			},
			args: args{
				maxRowsToReturn: 5,
			},
			want:    []int64{0, 1, 2, 3, 4},
			wantErr: false,
		},
		{
			name: "cursor advanced",
			fields: fields{
				pullExecutorBase: makeBase(),
				rows:             existingRows,
				count:            50,
				cursor:           5,
			},
			args: args{
				maxRowsToReturn: 100,
			},
			want:    []int64{5, 6, 7, 8, 9},
			wantErr: false,
		},
		{
			name: "cursor exhausted",
			fields: fields{
				pullExecutorBase: makeBase(),
				rows:             existingRows,
				count:            50,
				cursor:           10,
			},
			args: args{
				maxRowsToReturn: 100,
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "cursor exceeded",
			fields: fields{
				pullExecutorBase: makeBase(),
				rows:             existingRows,
				count:            50,
				cursor:           20,
			},
			args: args{
				maxRowsToReturn: 100,
			},
			want:    nil,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := &PullLimit{
				pullExecutorBase: tt.fields.pullExecutorBase,
				count:            tt.fields.count,
				offset:           tt.fields.offset,
				rows:             tt.fields.rows,
				cursor:           tt.fields.cursor,
				maxRows:          50000,
			}
			got, err := l.GetRows(tt.args.maxRowsToReturn)
			if (err != nil) != tt.wantErr {
				t.Errorf("PullLimit.GetRows() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if got.RowCount() != len(tt.want) {
					t.Errorf("expected %d rows but got %d", tt.want, got.RowCount())
				}
			}
			for i := 0; i < len(tt.want); i++ {
				row := got.GetRow(i)
				if row.GetInt64(0) != tt.want[i] {
					t.Errorf("row %d does not match %d", i, row.GetInt64(0))
				}
			}
		})
	}

}
