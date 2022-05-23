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
			colNames:       []string{"test_col"},
			colTypes:       colTypes,
			simpleColNames: common.ToSimpleColNames(colNames),
			rowsFactory:    rowsFactory,
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
		want    int
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
			want:    0,
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
			want:    0,
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
			want:    0,
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
			want:    1,
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
			want:    10,
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
			want:    5,
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
			}
			got, err := l.GetRows(tt.args.maxRowsToReturn)
			if (err != nil) != tt.wantErr {
				t.Errorf("PullLimit.GetRows() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			for i := 0; i < tt.want; i++ {
				row := got.GetRow(i)
				if row.GetInt64(0) != int64(i) {
					t.Errorf("row %d does not match %d", i, row.GetInt64(0))
				}
			}
		})
	}

}
