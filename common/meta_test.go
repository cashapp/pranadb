package common

import "testing"

func TestColumnType_String(t *testing.T) {
	type fields struct {
		Type         Type
		DecPrecision int
		DecScale     int
		FSP          int8
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name:   "tiny int",
			fields: fields{Type: TypeTinyInt},
			want:   "tinyint",
		},
		{
			name:   "int",
			fields: fields{Type: TypeInt},
			want:   "int",
		},
		{
			name:   "big int",
			fields: fields{Type: TypeBigInt},
			want:   "bigint",
		},
		{
			name:   "double",
			fields: fields{Type: TypeDouble},
			want:   "double",
		},
		{
			name:   "decimal",
			fields: fields{Type: TypeDecimal, DecPrecision: 4, DecScale: 8},
			want:   "decimal(4, 8)",
		},
		{
			name:   "varchar",
			fields: fields{Type: TypeVarchar},
			want:   "varchar",
		},
		{
			name:   "timestamp",
			fields: fields{Type: TypeTimestamp, FSP: 6},
			want:   "timestamp(6)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &ColumnType{
				Type:         tt.fields.Type,
				DecPrecision: tt.fields.DecPrecision,
				DecScale:     tt.fields.DecScale,
				FSP:          tt.fields.FSP,
			}
			if got := tr.String(); got != tt.want {
				t.Errorf("ColumnType.String() = %v, want %v", got, tt.want)
			}
		})
	}
}
