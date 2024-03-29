// Package shardlb implements algorithms to balance the leaders and load between nodes.
package shardlb

import (
	"fmt"
	"math"
	"math/rand"
	"strings"

	"github.com/squareup/pranadb/errors"
)

// Partition represents a data subset replicated across some nodes.
//
// Each replica has a score and belongs to a different node. One replica is the leader and the rest are followers.
// The rebalancer attempts to uniformize the total node scores.
type Partition struct {
	replicaScores []int
	leaderIndex   int
}

// NewPartition creates a new partition and validates the input data.
//
// The scores slice contains the score of each replica.
// Nodes without replicas must have a negative score.
// The leader index must point to a valid replica.
//
// Every partition must contain at least a node, a replica, and a leader.
func NewPartition(scores []int, leaderIndex int) (Partition, error) {
	partition := Partition{replicaScores: scores, leaderIndex: leaderIndex}
	if partition.Nodes() == 0 {
		return Partition{}, errors.New("cannot create partition with no nodes")
	}
	if partition.Replicas() == 0 {
		return Partition{}, errors.New("cannot create partition with no replicas")
	}
	if !(0 <= leaderIndex && leaderIndex < len(scores)) || scores[leaderIndex] < 0 {
		return Partition{}, errors.New("invalid leader index")
	}
	return partition, nil
}

// Nodes returns the number of nodes in the partition.
func (p *Partition) Nodes() int {
	return len(p.replicaScores)
}

// Replicas returns the number of replicas in the partition.
func (p *Partition) Replicas() int {
	count := 0
	for _, score := range p.replicaScores {
		if score >= 0 {
			count++
		}
	}
	return count
}

// SwapLeader changes the leader of the partition to the new index.
//
// The new index must point to a valid replica otherwise an error is returned.
func (p *Partition) SwapLeader(newLeaderIndex int) error {
	if !(0 <= newLeaderIndex && newLeaderIndex < p.Nodes()) || p.replicaScores[newLeaderIndex] < 0 {
		return errors.New("invalid new leader index")
	}
	p.replicaScores[p.leaderIndex], p.replicaScores[newLeaderIndex] = p.replicaScores[newLeaderIndex], p.replicaScores[p.leaderIndex]
	p.leaderIndex = newLeaderIndex
	return nil
}

// RandomFollowerIndex returns a random follower index or -1 if there are no followers.
func (p *Partition) RandomFollowerIndex(randSrc *rand.Rand) int {
	randomFollowerIndex, followers := -1, 0
	for replicaIndex, score := range p.replicaScores {
		if score < 0 || replicaIndex == p.leaderIndex {
			continue
		}
		followers++
		if followers == 1 {
			randomFollowerIndex = replicaIndex
		} else if randSrc.Intn(followers) == 0 {
			randomFollowerIndex = replicaIndex
		}
	}
	return randomFollowerIndex
}

func (p *Partition) String() string {
	return formatScores(p.replicaScores, p.leaderIndex)
}

// Cluster represents a collection of partitions.
type Cluster struct {
	partitions    []Partition
	perNodeScores []int
}

// NewCluster creates a cluster and validates the input data.
//
// A valid custer is comprised only of partitions with the same number of nodes and replicas.
func NewCluster(partitions []Partition) (Cluster, error) {
	nodes, replicas := -1, -1
	if len(partitions) == 0 {
		return Cluster{}, errors.New("cluster partitions cannot be empty")
	}
	for _, partition := range partitions {
		if nodes != -1 && nodes != partition.Nodes() {
			return Cluster{}, errors.New("cluster partitions must have equal node count")
		}
		if replicas != -1 && replicas != partition.Replicas() {
			return Cluster{}, errors.New("cluster partitions must have equal replica count")
		}
		nodes = partition.Nodes()
		replicas = partition.Replicas()
	}
	perNodeScores := make([]int, nodes)
	for _, partition := range partitions {
		for i, score := range partition.replicaScores {
			if score > 0 {
				perNodeScores[i] += score
			}
		}
	}
	return Cluster{partitions: partitions, perNodeScores: perNodeScores}, nil
}

// Partitions returns the number of partitions in the cluster.
func (c Cluster) Partitions() int {
	return len(c.partitions)
}

// Nodes returns the number of nodes in the cluster.
func (c Cluster) Nodes() int {
	if c.Partitions() == 0 {
		return 0
	}
	return c.partitions[0].Nodes()
}

// SwapLeader changes the leader of a partition.
//
// It returns a swap that records the leader change and can be undone.
func (c Cluster) SwapLeader(partitionIndex, newLeaderIndex int) (Swap, error) {
	if !(0 <= partitionIndex && partitionIndex < len(c.partitions)) {
		return Swap{}, errors.New("invalid partition index")
	}
	partition := &c.partitions[partitionIndex]
	if !(0 <= newLeaderIndex && newLeaderIndex < partition.Nodes()) {
		return Swap{}, errors.New("invalid new leader index")
	}
	oldLeaderIndex := partition.leaderIndex
	oldLeaderScore := partition.replicaScores[oldLeaderIndex]
	newLeaderScore := partition.replicaScores[newLeaderIndex]
	if err := partition.SwapLeader(newLeaderIndex); err != nil {
		return Swap{}, err
	}
	c.perNodeScores[oldLeaderIndex] += newLeaderScore - oldLeaderScore
	c.perNodeScores[newLeaderIndex] += -newLeaderScore + oldLeaderScore
	return Swap{
		PartitionIndex: partitionIndex,
		OldLeaderIndex: oldLeaderIndex,
		NewLeaderIndex: newLeaderIndex,
	}, nil
}

func (c Cluster) randomPartitionIndex(randSrc *rand.Rand, excludedPartitions map[int]struct{}) int {
	randomPartitionIndex, availablePartitions := -1, 0
	for partitionIndex := 0; partitionIndex < len(c.partitions); partitionIndex++ {
		if _, ok := excludedPartitions[partitionIndex]; ok {
			continue
		}
		availablePartitions++
		if availablePartitions == 1 {
			randomPartitionIndex = partitionIndex
		} else if randSrc.Intn(availablePartitions) == 0 {
			randomPartitionIndex = partitionIndex
		}
	}
	return randomPartitionIndex
}

// SwapRandom randomly changes the leader of a random partition.
//
// If all partitions indexes are excluded, or if there are no followers, a no-op swap is applied.
func (c Cluster) SwapRandom(randSrc *rand.Rand, excludedPartitionIndexes map[int]struct{}) Swap {
	noSwap := Swap{
		PartitionIndex: 0,
		OldLeaderIndex: c.partitions[0].leaderIndex,
		NewLeaderIndex: c.partitions[0].leaderIndex,
	}
	randomPartitionIndex := c.randomPartitionIndex(randSrc, excludedPartitionIndexes)
	if randomPartitionIndex < 0 {
		return noSwap
	}
	randomPartition := &c.partitions[randomPartitionIndex]
	newLeaderIndex := randomPartition.RandomFollowerIndex(randSrc)
	if newLeaderIndex < 0 { // no followers
		return noSwap
	}
	swap, _ := c.SwapLeader(randomPartitionIndex, newLeaderIndex)
	return swap
}

// Score computes the standard deviation of the total node scores.
func (c Cluster) Score() float64 {
	totalScore := 0
	for _, nodeScore := range c.perNodeScores {
		totalScore += nodeScore
	}
	nodes := float64(c.Nodes())
	meanScore := float64(totalScore) / nodes
	var variance float64
	for _, score := range c.perNodeScores {
		delta := float64(score) - meanScore
		variance += delta * delta
	}
	return math.Sqrt(variance / nodes)
}

func (c *Cluster) String() string {
	var result strings.Builder
	for _, partition := range c.partitions {
		fmt.Fprintf(&result, "%s\n", partition.String())
	}
	for i := 0; i < c.Nodes(); i++ {
		fmt.Fprintf(&result, "-------  ")
	}
	fmt.Fprintln(&result)
	fmt.Fprintf(&result, "%s -> %.2f\n", formatScores(c.perNodeScores, -1), c.Score())
	return result.String()
}

// Undo reverses a swap.
func (c Cluster) Undo(s Swap) error {
	_, err := c.SwapLeader(s.PartitionIndex, s.OldLeaderIndex)
	return err
}

// UndoSwaps reverses a series of swaps.
//
// The swaps are undone from right to left.
func (c Cluster) UndoSwaps(swaps Swaps) error {
	for i := len(swaps) - 1; i >= 0; i-- {
		if err := c.Undo(swaps[i]); err != nil {
			return err
		}
	}
	return nil
}

// SimulatedAnnealing uses a randomized algorithm to uniformize the score nodes.
//
// The algorithm applies limits only one leader swap per partition which
// requires some adjustments to the classic implementation.
func (c Cluster) SimulatedAnnealing(randSrc *rand.Rand, options ...SAOption) Swaps {
	config := defaultSAConfig
	config.applyOptions(options)
	var temperature float64 = 1
	currentScore := c.Score()
	excludedPartitions := make(map[int]struct{}, c.Partitions())
	swaps := make(Swaps, 0, c.Partitions())
	for i := 0; i < c.Partitions(); i++ { // Swap each partition once.
		temperature *= config.coolingFraction
		for j := 0; j < config.repeats; j++ {
			swap := c.SwapRandom(randSrc, excludedPartitions)
			newScore := c.Score()
			acceptWin := newScore < currentScore
			acceptLoss := false
			if newScore > currentScore { // Accept a worse score?
				exponent := (1 - newScore/currentScore) / (config.k * temperature)
				acceptLoss = math.Exp(exponent) > randSrc.Float64()
			}
			if acceptWin || acceptLoss {
				currentScore = newScore
				swaps = append(swaps, swap)
				excludedPartitions[swap.PartitionIndex] = struct{}{}
				break
			} else {
				_ = c.Undo(swap)
			}
		}
	}
	return swaps
}

// Rebalance the cluster by running multiple rounds of simulated annealing and keeping the best result.
func (c Cluster) Rebalance(r *rand.Rand, simulations int, options ...SAOption) Swaps {
	bestScore := c.Score()
	var bestSwaps Swaps
	for i := 0; i < simulations; i++ {
		swaps := c.SimulatedAnnealing(r, options...)
		newScore := c.Score()
		if newScore < bestScore {
			bestScore = newScore
			bestSwaps = swaps
		} else if newScore == bestScore && len(swaps) < len(bestSwaps) {
			bestScore = newScore
			bestSwaps = swaps
		}
		_ = c.UndoSwaps(swaps)
	}
	for _, swap := range bestSwaps {
		_, _ = c.SwapLeader(swap.PartitionIndex, swap.NewLeaderIndex)
	}
	return bestSwaps
}

// Swap represents a leader change applied to the cluster.
type Swap struct {
	PartitionIndex int
	OldLeaderIndex int
	NewLeaderIndex int
}

func (s Swap) String() string {
	return fmt.Sprintf("%d:%d->%d", s.PartitionIndex, s.OldLeaderIndex, s.NewLeaderIndex)
}

type Swaps []Swap

func (s Swaps) String() string {
	swaps := make([]string, 0, len(s))
	for _, swap := range s {
		swaps = append(swaps, swap.String())
	}
	return strings.Join(swaps, ", ")
}

type saConfig struct {
	coolingFraction float64
	k               float64
	repeats         int
}

func (c *saConfig) applyOptions(options []SAOption) {
	for _, option := range options {
		option(c)
	}
}

var defaultSAConfig = saConfig{
	coolingFraction: .8,
	k:               1,
	repeats:         5,
}

type SAOption func(*saConfig)

// SACoolingFraction controls the temperature drop of the simulated annealing.
func SACoolingFraction(coolingFraction float64) SAOption {
	return func(config *saConfig) {
		if coolingFraction > 1 {
			coolingFraction = 1
		}
		if coolingFraction < 0 {
			coolingFraction = 0
		}
		config.coolingFraction = coolingFraction
	}
}

// SAK controls the Boltzmann’s constant.
func SAK(k float64) SAOption {
	return func(c *saConfig) {
		c.k = k
	}
}

// SARetries controls how many times a random leader swap is attempted at the same temperature.
func SARetries(repeats int) SAOption {
	return func(c *saConfig) {
		c.repeats = repeats
	}
}

func formatScores(scores []int, leaderIndex int) string {
	var result strings.Builder
	for scoreIndex, score := range scores {
		if score >= 0 {
			fmt.Fprintf(&result, "%7d", score)
		} else {
			result.WriteString("       ")
		}
		if scoreIndex == leaderIndex {
			result.WriteString("* ")
		} else {
			result.WriteString("  ")
		}
	}
	return result.String()
}
