package shardlb

import (
	"math"
	"math/rand"
	"reflect"
	"testing"
)

func must(c Cluster, err error) Cluster {
	if err != nil {
		panic(err.Error())
	}
	return c
}

func TestNewPartition(t *testing.T) {
	type args struct {
		scores      []int
		leaderIndex int
	}
	tests := []struct {
		name    string
		args    args
		want    Partition
		wantErr bool
	}{
		{
			name: "no followers",
			args: args{
				scores:      []int{1},
				leaderIndex: 0,
			},
			want: Partition{replicaScores: []int{1}, leaderIndex: 0},
		},
		{
			name:    "no scores",
			wantErr: true,
		},
		{
			name: "negative leader index",
			args: args{
				scores:      []int{1, 2, 3},
				leaderIndex: -1,
			},
			wantErr: true,
		},
		{
			name: "bad index leader",
			args: args{
				scores:      []int{1, 2, 3},
				leaderIndex: 10,
			},
			wantErr: true,
		},
		{
			name: "success",
			args: args{
				scores:      []int{1, 2, 3},
				leaderIndex: 1,
			},
			want: Partition{replicaScores: []int{1, 2, 3}, leaderIndex: 1},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := NewPartition(test.args.scores, test.args.leaderIndex)
			if (err != nil) != test.wantErr {
				t.Errorf("NewPartition() error = %v, wantErr %v", err, test.wantErr)
				return
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("NewPartition() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestPartition_Nodes(t *testing.T) {
	type fields struct {
		scores      []int
		leaderIndex int
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		{
			name:   "3 nodes",
			fields: fields{scores: []int{1, 2, -1}, leaderIndex: 0},
			want:   3,
		},
		{
			name:   "5 nodes",
			fields: fields{scores: []int{1, 2, -1, 3, -1}, leaderIndex: 0},
			want:   5,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := &Partition{
				replicaScores: test.fields.scores,
				leaderIndex:   test.fields.leaderIndex,
			}
			if got := p.Nodes(); got != test.want {
				t.Errorf("Partition.Nodes() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestPartition_Replicas(t *testing.T) {
	type fields struct {
		scores      []int
		leaderIndex int
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		{
			name:   "3 replicas",
			fields: fields{scores: []int{1, 2, -1, 0, -1}, leaderIndex: 0},
			want:   3,
		},
		{
			name:   "5 replicas",
			fields: fields{scores: []int{1, 2, -1, 0, -1, 0, 2}, leaderIndex: 0},
			want:   5,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := &Partition{
				replicaScores: test.fields.scores,
				leaderIndex:   test.fields.leaderIndex,
			}
			if got := p.Replicas(); got != test.want {
				t.Errorf("Partition.Replicas() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestPartition_SwapLeader(t *testing.T) {
	type fields struct {
		scores      []int
		leaderIndex int
	}
	type args struct {
		newLeaderIndex int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		want    Partition
	}{
		{
			name:   "swap leader with itself",
			fields: fields{scores: []int{0, 1, -1, 2, -1}, leaderIndex: 0},
			args:   args{newLeaderIndex: 0},
			want:   Partition{replicaScores: []int{0, 1, -1, 2, -1}, leaderIndex: 0},
		},
		{
			name:   "swap",
			fields: fields{scores: []int{0, 1, -1, 2, -1}, leaderIndex: 0},
			args:   args{newLeaderIndex: 1},
			want:   Partition{replicaScores: []int{1, 0, -1, 2, -1}, leaderIndex: 1},
		},
		{
			name:    "swap with non-shard",
			fields:  fields{scores: []int{0, 1, -1, 2, -1}, leaderIndex: 0},
			args:    args{newLeaderIndex: 2},
			wantErr: true,
			want:    Partition{replicaScores: []int{0, 1, -1, 2, -1}, leaderIndex: 0},
		},
		{
			name:    "swap with loader out of bounds",
			fields:  fields{scores: []int{0, 1, -1, 2, -1}, leaderIndex: 0},
			args:    args{newLeaderIndex: 10},
			wantErr: true,
			want:    Partition{replicaScores: []int{0, 1, -1, 2, -1}, leaderIndex: 0},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			partition := Partition{
				replicaScores: test.fields.scores,
				leaderIndex:   test.fields.leaderIndex,
			}
			if err := partition.SwapLeader(test.args.newLeaderIndex); (err != nil) != test.wantErr {
				t.Errorf("Partition.SwapLeader() error = %v, wantErr %v", err, test.wantErr)
				return
			}
			if !reflect.DeepEqual(partition, test.want) {
				t.Errorf("NewPartition() = %v, want %v", partition, test.want)
			}
		})
	}
}

func randomPartition(r *rand.Rand) Partition {
	scores := []int{-1, -1, 5 + r.Intn(10), 5 + r.Intn(10), 5 + r.Intn(10), 5 + r.Intn(10), 5 + r.Intn(10)*3}
	rand.Shuffle(7, func(i, j int) { scores[i], scores[j] = scores[j], scores[i] })
	leaderIndex, maxScore := 0, scores[0]
	for i := 1; i < 7; i++ {
		if scores[i] > maxScore {
			leaderIndex = i
			maxScore = scores[i]
		}
	}
	partition, _ := NewPartition(scores, leaderIndex)
	return partition
}

func randomCluster(r *rand.Rand, n int) Cluster {
	partitions := make([]Partition, 0, n)
	for i := 0; i < n; i++ {
		partitions = append(partitions, randomPartition(r))
	}
	cluster, _ := NewCluster(partitions)
	return cluster
}

func BenchmarkCluster_SimulatedAnnealing20(b *testing.B) {
	benchmarkClusterSimulatedAnnealing(b, 20)
}

func BenchmarkCluster_SimulatedAnnealing100(b *testing.B) {
	benchmarkClusterSimulatedAnnealing(b, 100)
}

func BenchmarkCluster_SimulatedAnnealing200(b *testing.B) {
	benchmarkClusterSimulatedAnnealing(b, 200)
}

func BenchmarkCluster_SimulatedAnnealing400(b *testing.B) {
	benchmarkClusterSimulatedAnnealing(b, 400)
}

func benchmarkClusterSimulatedAnnealing(b *testing.B, n int) {
	b.Helper()
	r := rand.New(rand.NewSource(0))
	c := randomCluster(r, n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		swaps := c.SimulatedAnnealing(r)
		b.StopTimer()
		if err := c.UndoSwaps(swaps); err != nil {
			b.FailNow()
		}
		b.StartTimer()
	}
}

func TestNewCluster(t *testing.T) {
	type args struct {
		partitions []Partition
	}
	tests := []struct {
		name    string
		args    args
		want    Cluster
		wantErr bool
	}{
		{
			name: "no partitions",
			args: args{
				partitions: []Partition{},
			},
			wantErr: true,
		},
		{
			name: "partitions with different node count",
			args: args{
				partitions: []Partition{
					{replicaScores: []int{0, 0, 0, -1, -1}, leaderIndex: 0},
					{replicaScores: []int{0, 0, 0, -1, -1, -1}, leaderIndex: 0},
				},
			},
			wantErr: true,
		},
		{
			name: "partitions with different replica count",
			args: args{
				partitions: []Partition{
					{replicaScores: []int{0, 0, 0, 0, -1}, leaderIndex: 0},
					{replicaScores: []int{0, 0, 0, -1, -1}, leaderIndex: 0},
				},
			},
			wantErr: true,
		},
		{
			name: "success",
			args: args{
				partitions: []Partition{
					{replicaScores: []int{1, 3, -1, 4, -1}, leaderIndex: 0},
					{replicaScores: []int{2, 0, 0, -1, -1}, leaderIndex: 0},
				},
			},
			want: Cluster{
				partitions: []Partition{
					{replicaScores: []int{1, 3, -1, 4, -1}, leaderIndex: 0},
					{replicaScores: []int{2, 0, 0, -1, -1}, leaderIndex: 0},
				},
				perNodeScores: []int{3, 3, 0, 4, 0},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := NewCluster(test.args.partitions)
			if (err != nil) != test.wantErr {
				t.Errorf("NewCluster() error = %v, wantErr %v", err, test.wantErr)
				return
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("NewCluster() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestCluster_Partitions(t *testing.T) {
	cluster := must(NewCluster(
		[]Partition{
			{replicaScores: []int{1, 3, -1, 4, -1}, leaderIndex: 0},
			{replicaScores: []int{2, 0, 0, -1, -1}, leaderIndex: 0},
			{replicaScores: []int{0, 0, 0, -1, -1}, leaderIndex: 0},
		},
	))
	if cluster.Partitions() != 3 {
		t.Fatalf("Cluster.Partitions() = %v, want %v", cluster.Partitions(), 3)
	}
}

func TestCluster_Nodes(t *testing.T) {
	cluster := must(NewCluster(
		[]Partition{
			{replicaScores: []int{1, 3, -1, 4, -1}, leaderIndex: 0},
			{replicaScores: []int{2, 0, 0, -1, -1}, leaderIndex: 0},
			{replicaScores: []int{0, 0, 0, -1, -1}, leaderIndex: 0},
		},
	))
	if cluster.Nodes() != 5 {
		t.Fatalf("Cluster.Nodes() = %v, want %v", cluster.Nodes(), 5)
	}
}

func TestCluster_SwapLeader(t *testing.T) {
	type args struct {
		partitionIndex int
		newLeaderIndex int
	}
	type want struct {
		swap    Swap
		cluster Cluster
	}
	tests := []struct {
		name    string
		cluster Cluster
		args    args
		want    want
		wantErr bool
	}{
		{
			name: "negative partition index",
			cluster: must(NewCluster([]Partition{
				{replicaScores: []int{1, 3, -1, 4, -1}, leaderIndex: 0},
				{replicaScores: []int{2, 0, 0, -1, -1}, leaderIndex: 0},
				{replicaScores: []int{0, 0, 0, -1, -1}, leaderIndex: 0},
			})),
			args:    args{partitionIndex: -1, newLeaderIndex: 0},
			wantErr: true,
		},
		{
			name: "large partition index",
			cluster: must(NewCluster([]Partition{
				{replicaScores: []int{1, 3, -1, 4, -1}, leaderIndex: 0},
				{replicaScores: []int{2, 0, 0, -1, -1}, leaderIndex: 0},
				{replicaScores: []int{0, 0, 0, -1, -1}, leaderIndex: 0},
			})),
			args:    args{partitionIndex: 10, newLeaderIndex: 0},
			wantErr: true,
		},
		{
			name: "swap with non shard",
			cluster: must(NewCluster([]Partition{
				{replicaScores: []int{1, 3, -1, 4, -1}, leaderIndex: 0},
				{replicaScores: []int{2, 0, 0, -1, -1}, leaderIndex: 0},
				{replicaScores: []int{0, 0, 0, -1, -1}, leaderIndex: 0},
			})),
			args:    args{partitionIndex: 0, newLeaderIndex: 2},
			wantErr: true,
		},
		{
			name: "swap with negative follower index",
			cluster: must(NewCluster([]Partition{
				{replicaScores: []int{1, 3, -1, 4, -1}, leaderIndex: 0},
				{replicaScores: []int{2, 0, 0, -1, -1}, leaderIndex: 0},
				{replicaScores: []int{0, 0, 0, -1, -1}, leaderIndex: 0},
			})),
			args:    args{partitionIndex: 0, newLeaderIndex: -1},
			wantErr: true,
		},
		{
			name: "swap with large follower index",
			cluster: must(NewCluster([]Partition{
				{replicaScores: []int{1, 3, -1, 4, -1}, leaderIndex: 0},
				{replicaScores: []int{2, 0, 0, -1, -1}, leaderIndex: 0},
				{replicaScores: []int{0, 0, 0, -1, -1}, leaderIndex: 0},
			})),
			args:    args{partitionIndex: 0, newLeaderIndex: 10},
			wantErr: true,
		},
		{
			name: "success",
			cluster: must(NewCluster([]Partition{
				{replicaScores: []int{1, 3, -1, 4, -1}, leaderIndex: 0},
				{replicaScores: []int{2, 0, 0, -1, -1}, leaderIndex: 0},
				{replicaScores: []int{0, 0, 0, -1, -1}, leaderIndex: 0},
			})),
			args: args{partitionIndex: 0, newLeaderIndex: 1},
			want: want{
				swap: Swap{PartitionIndex: 0, NewLeaderIndex: 1, OldLeaderIndex: 0},
				cluster: must(NewCluster([]Partition{
					{replicaScores: []int{3, 1, -1, 4, -1}, leaderIndex: 1},
					{replicaScores: []int{2, 0, 0, -1, -1}, leaderIndex: 0},
					{replicaScores: []int{0, 0, 0, -1, -1}, leaderIndex: 0},
				})),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.cluster.SwapLeader(test.args.partitionIndex, test.args.newLeaderIndex)
			if (err != nil) != test.wantErr {
				t.Errorf("Cluster.SwapLeader() error = %v, wantErr %v", err, test.wantErr)
				return
			}
			if !reflect.DeepEqual(got, test.want.swap) {
				t.Errorf("Cluster.SwapLeader() = %v, want %v", got, test.want)
			}
			if !reflect.DeepEqual(test.want.cluster, Cluster{partitions: []Partition{}, perNodeScores: []int{}}) {
				if !reflect.DeepEqual(test.cluster, test.want.cluster) {
					t.Errorf("Cluster.SwapLeader() = %v, want %v", test.cluster, test.want.cluster)
				}
			}
		})
	}
}

func TestCluster_Score(t *testing.T) {
	cluster := must(NewCluster(
		[]Partition{
			{replicaScores: []int{1, 3, -1, 4, -1}, leaderIndex: 0},
			{replicaScores: []int{2, 0, 0, -1, -1}, leaderIndex: 0},
			{replicaScores: []int{0, 0, 0, -1, -1}, leaderIndex: 0},
			//             3  3  0  4   0 -> sqrt((1+1+4+4+4)/5)
		},
	))
	score := math.Sqrt(float64(14) / 5)
	if cluster.Score() != score {
		t.Fatalf("Cluster.Score() = %v, want %v", cluster.Score(), score)
	}
}

func TestCluster_Undo(t *testing.T) {
	type args struct {
		s Swap
	}
	tests := []struct {
		name    string
		cluster Cluster
		args    args
		wantErr bool
		want    Cluster
	}{
		{
			name: "swap with negative partition index",
			cluster: must(NewCluster([]Partition{
				{replicaScores: []int{1, 3, -1, 4, -1}, leaderIndex: 0},
				{replicaScores: []int{2, 0, 0, -1, -1}, leaderIndex: 0},
				{replicaScores: []int{0, 0, 0, -1, -1}, leaderIndex: 0},
			})),
			args:    args{s: Swap{PartitionIndex: -1, NewLeaderIndex: 0, OldLeaderIndex: 1}},
			wantErr: true,
		},
		{
			name: "swap with large partition index",
			cluster: must(NewCluster([]Partition{
				{replicaScores: []int{1, 3, -1, 4, -1}, leaderIndex: 0},
				{replicaScores: []int{2, 0, 0, -1, -1}, leaderIndex: 0},
				{replicaScores: []int{0, 0, 0, -1, -1}, leaderIndex: 0},
			})),
			args:    args{s: Swap{PartitionIndex: 10, NewLeaderIndex: 0, OldLeaderIndex: 1}},
			wantErr: true,
		},
		{
			name: "swap with negative old leader index",
			cluster: must(NewCluster([]Partition{
				{replicaScores: []int{1, 3, -1, 4, -1}, leaderIndex: 0},
				{replicaScores: []int{2, 0, 0, -1, -1}, leaderIndex: 0},
				{replicaScores: []int{0, 0, 0, -1, -1}, leaderIndex: 0},
			})),
			args:    args{s: Swap{PartitionIndex: 0, NewLeaderIndex: 0, OldLeaderIndex: -1}},
			wantErr: true,
		},
		{
			name: "swap with large old leader index",
			cluster: must(NewCluster([]Partition{
				{replicaScores: []int{1, 3, -1, 4, -1}, leaderIndex: 0},
				{replicaScores: []int{2, 0, 0, -1, -1}, leaderIndex: 0},
				{replicaScores: []int{0, 0, 0, -1, -1}, leaderIndex: 0},
			})),
			args:    args{s: Swap{PartitionIndex: 0, NewLeaderIndex: 0, OldLeaderIndex: 10}},
			wantErr: true,
		},
		{
			name: "success",
			cluster: must(NewCluster([]Partition{
				{replicaScores: []int{1, 3, -1, 4, -1}, leaderIndex: 0},
				{replicaScores: []int{2, 0, 0, -1, -1}, leaderIndex: 0},
				{replicaScores: []int{0, 0, 0, -1, -1}, leaderIndex: 0},
			})),
			args: args{s: Swap{PartitionIndex: 0, NewLeaderIndex: 0, OldLeaderIndex: 1}},
			want: must(NewCluster([]Partition{
				{replicaScores: []int{3, 1, -1, 4, -1}, leaderIndex: 1},
				{replicaScores: []int{2, 0, 0, -1, -1}, leaderIndex: 0},
				{replicaScores: []int{0, 0, 0, -1, -1}, leaderIndex: 0},
			})),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.cluster.Undo(test.args.s); (err != nil) != test.wantErr {
				t.Errorf("Cluster.Undo() error = %v, wantErr %v", err, test.wantErr)
			}
			if !reflect.DeepEqual(test.want, Cluster{partitions: []Partition{}, perNodeScores: []int{}}) {
				if !reflect.DeepEqual(test.cluster, test.want) {
					t.Errorf("Cluster.SwapLeader() = %v, want %v", test.cluster, test.want)
				}
			}
		})
	}
}

func TestCluster_SimulatedAnnealing(t *testing.T) {
	r := rand.New(rand.NewSource(0))
	for i := 0; i < 100; i++ {
		c := randomCluster(r, 100)
		startScore := c.Score()
		c.SimulatedAnnealing(r)
		if !(c.Score() < startScore) {
			t.Errorf("Cluster.SimulatedAnnealing() = %v, start score = %v", c.Score(), startScore)
		}
	}
}

func TestCluster_Rebalance(t *testing.T) {
	r := rand.New(rand.NewSource(0))
	for i := 0; i < 100; i++ {
		c := randomCluster(r, 100)
		startScore := c.Score()
		c.Rebalance(r, 100, SACoolingFraction(0.6), SAK(10), SARetries(3))
		if !(c.Score() < startScore) {
			t.Errorf("Cluster.Rebalance() = %v, start score = %v", c.Score(), startScore)
		}
	}
}

func TestPartition_RandomFollowerIndex(t *testing.T) {
	type fields struct {
		replicaScores []int
		leaderIndex   int
	}
	type args struct {
		r *rand.Rand
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		{
			name: "no followers",
			fields: fields{
				replicaScores: []int{0, -1, -1},
				leaderIndex:   0,
			},
			args: args{r: rand.New(rand.NewSource(0))},
			want: -1,
		},
		{
			name: "one follower",
			fields: fields{
				replicaScores: []int{0, 1, -1},
				leaderIndex:   0,
			},
			args: args{r: rand.New(rand.NewSource(0))},
			want: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := &Partition{
				replicaScores: test.fields.replicaScores,
				leaderIndex:   test.fields.leaderIndex,
			}
			if got := p.RandomFollowerIndex(test.args.r); got != test.want {
				t.Errorf("Partition.RandomFollowerIndex() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestCluster_SwapRandom(t *testing.T) {
	type args struct {
		r                        *rand.Rand
		excludedPartitionIndexes map[int]struct{}
	}
	tests := []struct {
		name    string
		cluster Cluster
		args    args
		want    Swap
	}{
		{
			name: "all partitions excluded",
			cluster: must(NewCluster([]Partition{
				{replicaScores: []int{1, 3, -1, 4, -1}, leaderIndex: 0},
				{replicaScores: []int{2, 0, 0, -1, -1}, leaderIndex: 0},
				{replicaScores: []int{0, 0, 0, -1, -1}, leaderIndex: 0},
			})),
			args: args{
				r:                        rand.New(rand.NewSource(0)),
				excludedPartitionIndexes: map[int]struct{}{0: {}, 1: {}, 2: {}},
			},
			want: Swap{PartitionIndex: 0, OldLeaderIndex: 0, NewLeaderIndex: 0},
		},
		{
			name: "all partitions excluded but one",
			cluster: must(NewCluster([]Partition{
				{replicaScores: []int{1, 3, -1, -1, -1}, leaderIndex: 0},
				{replicaScores: []int{2, 0, -1, -1, -1}, leaderIndex: 0},
				{replicaScores: []int{0, 0, -1, -1, -1}, leaderIndex: 0},
			})),
			args: args{
				r:                        rand.New(rand.NewSource(0)),
				excludedPartitionIndexes: map[int]struct{}{0: {}, 2: {}},
			},
			want: Swap{PartitionIndex: 1, OldLeaderIndex: 0, NewLeaderIndex: 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cluster.SwapRandom(tt.args.r, tt.args.excludedPartitionIndexes); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Cluster.SwapRandom() = %v, want %v", got, tt.want)
			}
		})
	}
}
