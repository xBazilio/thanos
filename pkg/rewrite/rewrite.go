package rewrite

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
	"github.com/thanos-io/thanos/pkg/compactv2"
	"github.com/thanos-io/thanos/pkg/errutil"
	"github.com/thanos-io/thanos/pkg/runutil"
)

type sample struct {
	t int64
	v float64
}

func RewriteDownsampled(
	logger log.Logger,
	uid ulid.ULID,
	origMeta *metadata.Meta,
	b tsdb.BlockReader,
	dir string,
	resolution int64,
	p compactv2.ProgressLogger,
	deletions []metadata.DeletionRequest,
) (err error) {
	if origMeta.Thanos.Downsample.Resolution == downsample.ResLevel0 {
		return errors.New("RewriteDownsampled can't be called on raw block")
	}

	indexr, err := b.Index()
	if err != nil {
		return errors.Wrap(err, "open index reader")
	}
	defer runutil.CloseWithErrCapture(&err, indexr, "downsample index reader")

	chunkr, err := b.Chunks()
	if err != nil {
		return errors.Wrap(err, "open chunk reader")
	}
	defer runutil.CloseWithErrCapture(&err, chunkr, "downsample chunk reader")

	// Create block directory to populate with chunks, meta and index files into.
	blockDir := filepath.Join(dir, uid.String())
	if err := os.MkdirAll(blockDir, 0750); err != nil {
		return errors.Wrap(err, "mkdir block dir")
	}

	// Remove blockDir in case of errors.
	defer func() {
		if err != nil {
			var merr errutil.MultiError
			merr.Add(err)
			merr.Add(os.RemoveAll(blockDir))
			err = merr.Err()
		}
	}()

	newMeta := *origMeta
	newMeta.ULID = uid
	newMeta.Thanos.Labels["block_source"] = "bucket-rewrite"

	// Writes downsampled chunks right into the files, avoiding excess memory allocation.
	// Flushes index and meta data after aggregations.
	streamedBlockWriter, err := downsample.NewStreamedBlockWriter(blockDir, indexr, logger, newMeta)
	if err != nil {
		return errors.Wrap(err, "get streamed block writer")
	}
	defer runutil.CloseWithErrCapture(&err, streamedBlockWriter, "close stream block writer")

	postings, err := indexr.Postings(index.AllPostingsKey())
	if err != nil {
		return errors.Wrap(err, "get all postings list")
	}

	var (
		aggrChunks []*downsample.AggrChunk
		all        []sample
		chks       []chunks.Meta
		builder    labels.ScratchBuilder
	)
	for postings.Next() {
		chks = chks[:0]
		all = all[:0]
		aggrChunks = aggrChunks[:0]

		// Get series labels and chunks.
		if err := indexr.Series(postings.At(), &builder, &chks); err != nil {
			return errors.Wrapf(err, "get series %d", postings.At())
		}

		for i, c := range chks[1:] {
			if chks[i].MaxTime >= c.MinTime {
				return errors.Errorf("found overlapping chunks within series %d. Chunks expected to be ordered by min time and non-overlapping, got: %v", postings.At(), chks)
			}
		}

		lset := builder.Labels()

		// apply deletions
		if deleted := applyDeletions(lset, deletions); deleted {
			p.SeriesProcessed()
			continue
		}

		// read chunks data
		for i, c := range chks {
			chk, err := chunkr.Chunk(c)
			if err != nil {
				return errors.Wrapf(err, "get chunk %d, series %d", c.Ref, postings.At())
			}
			chks[i].Chunk = chk
		}

		// convert chunks to downsample.AggrChunk
		for _, c := range chks {
			ac, ok := c.Chunk.(*downsample.AggrChunk)
			if !ok {
				level.Warn(logger).Log("msg", fmt.Sprintf("expected downsampled chunk (*downsample.AggrChunk) got an empty %T instead for series: %d", c.Chunk, postings.At()))
				continue
			}
			aggrChunks = append(aggrChunks, ac)
		}
		if len(aggrChunks) == 0 {
			continue
		}
		downsampledChunks, err := downsampleAggr(
			logger,
			aggrChunks,
			&all,
			chks[0].MinTime,
			chks[len(chks)-1].MaxTime,
			origMeta.Thanos.Downsample.Resolution,
			resolution,
		)
		if err != nil {
			return errors.Wrapf(err, "downsample aggregate block, series: %d", postings.At())
		}
		if err := streamedBlockWriter.WriteSeries(builder.Labels(), downsampledChunks); err != nil {
			return errors.Wrapf(err, "write series: %d", postings.At())
		}

		p.SeriesProcessed()
	}

	return nil
}

func applyDeletions(
	lbls labels.Labels,
	deletions []metadata.DeletionRequest,
) bool {
	if len(deletions) == 0 {
		return false
	}
DeletionsLoop:
	for _, deletion := range deletions {
		for _, m := range deletion.Matchers {
			v := lbls.Get(m.Name)

			// v == "" means Matcher doesn't have labels for this label set
			// if v is not empy, then Matcher should match to delete series with label set `lbls`
			if v == "" || !m.Matches(v) {
				continue DeletionsLoop
			}
		}

		// all matchers have matched. procede to deletion
		return true
	}
	// no matches, keep series
	return false
}

func getGoodChunks(logger log.Logger, part []*downsample.AggrChunk) []*downsample.AggrChunk {
	goodPart := make([]*downsample.AggrChunk, 0, len(part))
rangePartLoop:
	for _, c := range part {
		var iters [2]chunkenc.Iterator
		// we need only 0th and 1st AggrType, but we iterate through all 5 to catch error "invalid size"
		for i := 0; i < 5; i++ {
			ac, err := c.Get(downsample.AggrType(i))
			if err != nil {
				level.Error(logger).Log("msg", "skip chunk part", "err", err)
				continue rangePartLoop
			}
			if i < 2 {
				iters[i] = ac.Iterator(nil)
			}
		}

		// create iterator to check samples
		ai := downsample.NewAverageChunkIterator(iters[0], iters[1])
		// exhaust iterator and...
		for ai.Next() != chunkenc.ValNone {
		}
		// ...check its error
		if err := ai.Err(); err != nil {
			// try to fix the wreched chunk
			ab, err := fixChunk(c)
			if err != nil {
				level.Error(logger).Log("msg", "skip chunk part", "err", err)
				continue rangePartLoop
			}
			c = downsample.EncodeAggrChunk(ab.chunks)
		}

		goodPart = append(goodPart, c)
	}
	return goodPart
}

func fixChunk(chk *downsample.AggrChunk) (*aggrChunkBuilder, error) {
	ab := &aggrChunkBuilder{}
	var reuseIt chunkenc.Iterator
	var samplesCnt [5]int
	var samples [5][]sample
	var samplesFixed [5][]sample

	for i := 0; i < 5; i++ {
		c, err := chk.Get(downsample.AggrType(i))
		if err != nil {
			return nil, err
		}
		if err := expandChunkIterator(c.Iterator(reuseIt), &samples[i]); err != nil {
			return nil, err
		}
	}

	// problem: AggrCount has extra sample in the beginning
	if len(samples[0])-len(samples[1]) == 1 && samples[0][1].t == samples[1][0].t {
		// solve problem by deleting extra sample
		samples[0] = samples[0][1:]
	}

	// problem: t in samples is too huge
	timeNow := time.Now().UnixMilli()
	if samples[4][0].t > timeNow {
		return nil, errors.Errorf("time is too huge: %d", samples[4][0].t)
	}

	// problem: AggrCouner is too small
	if len(samples[4]) < len(samples[0]) {
		return nil, errors.Errorf("AggrCounter is too small")
	}

	// problem: samples timestamps are not aligned
	// How it looks like:
	// .  .  .
	// .   . .
	// How we fix it? We add extra samples, to align timestamps
	// so it will look like this:
	// .  .. .
	// .  .. .
	// after that downsample algorithm will make it into
	// .  .  .
	// .  .  .

	// First: we collect all timestamps and sort them
	// ignore AggrCounter timestamps. It can have extra.
	var tms []int64
	tmset := make(map[int64]struct{})
	for i := 0; i < 4; i++ {
		for _, s := range samples[i] {
			tmset[s.t] = struct{}{}
		}
	}
	for t, _ := range tmset {
		tms = append(tms, t)
	}
	sort.Slice(tms, func(a, b int) bool { return tms[a] < tms[b] })

	// Second: for every timestamp we add value
	var idxs [4]int
	for _, t := range tms {
		for i := 0; i < 4; i++ {
			if t == samples[i][idxs[i]].t {
				// if chunk has a value for the timestamp: add that value
				samplesFixed[i] = append(samplesFixed[i], samples[i][idxs[i]])
				idxs[i]++
			} else {
				// if chunk doesn't have a value for the timestamp: get previous value
				// potentially this solution can cause data to be incorrect, but we don't have other options
				idx := idxs[i] - 1
				if idx < 0 {
					idx = 0
				}
				samplesFixed[i] = append(samplesFixed[i], sample{t, samples[i][idx].v})
			}
		}

		// don't add extra samples
		if idxs[0] == len(samples[0]) {
			break
		}
	}

	// just copy AggrCounter intact
	samplesFixed[4] = samples[4]

	// reassemble AggrChunk with timestamps aligned
	for i := 0; i < 5; i++ {
		ab.chunks[i] = chunkenc.NewXORChunk()
		ab.apps[i], _ = ab.chunks[i].Appender()
		for _, s := range samplesFixed[i] {
			ab.apps[i].Append(s.t, s.v)
			samplesCnt[i]++
		}
	}

	// sanity check
	for i := 1; i < 4; i++ {
		if samplesCnt[i-1] != samplesCnt[i] {
			return nil, fmt.Errorf("fixChunk: wrong samples count for %d and %d", i-1, i)
		}
	}

	return ab, nil
}

// ⬇️⬇️⬇️⬇️⬇️⬇️⬇️⬇️⬇️⬇️⬇️⬇️⬇️⬇️⬇️⬇️⬇️⬇️⬇️⬇️⬇️ //
// bunch of code copied from downsample package //

// currentWindow returns the end timestamp of the window that t falls into.
func currentWindow(t, r int64) int64 {
	// The next timestamp is the next number after s.t that's aligned with window.
	// We subtract 1 because block ranges are [from, to) and the last sample would
	// go out of bounds otherwise.
	return t - (t % r) + r - 1
}

// aggregator collects cumulative stats for a stream of values.
type aggregator struct {
	total   int     // Total samples processed.
	count   int     // Samples in current window.
	sum     float64 // Value sum of current window.
	min     float64 // Min of current window.
	max     float64 // Max of current window.
	counter float64 // Total counter state since beginning.
	resets  int     // Number of counter resets since beginning.
	last    float64 // Last added value.
}

// reset the stats to start a new aggregation window.
func (a *aggregator) reset() {
	a.count = 0
	a.sum = 0
	a.min = math.MaxFloat64
	a.max = -math.MaxFloat64
}

func (a *aggregator) add(v float64) {
	if a.total > 0 {
		if v < a.last {
			// Counter reset, correct the value.
			a.counter += v
			a.resets++
		} else {
			// Add delta with last value to the counter.
			a.counter += v - a.last
		}
	} else {
		// First sample sets the counter.
		a.counter = v
	}
	a.last = v

	a.sum += v
	a.count++
	a.total++

	if v < a.min {
		a.min = v
	}
	if v > a.max {
		a.max = v
	}
}

// downsampleBatch aggregates the data over the given resolution and calls add each time
// the end of a resolution was reached.
func downsampleBatch(data []sample, resolution int64, add func(int64, *aggregator)) int64 {
	var (
		aggr  aggregator
		nextT = int64(-1)
		lastT = data[len(data)-1].t
	)
	// Fill up one aggregate chunk with up to m samples.
	for _, s := range data {
		if value.IsStaleNaN(s.v) {
			continue
		}
		if s.t > nextT {
			if nextT != -1 {
				add(nextT, &aggr)
			}
			aggr.reset()
			nextT = currentWindow(s.t, resolution)
			// Limit next timestamp to not go beyond the batch. A subsequent batch
			// may overlap in time range otherwise.
			// We have aligned batches for raw downsamplings but subsequent downsamples
			// are forced to be chunk-boundary aligned and cannot guarantee this.
			if nextT > lastT {
				nextT = lastT
			}
		}
		aggr.add(s.v)
	}
	// Add the last sample.
	add(nextT, &aggr)

	return nextT
}

// aggrChunkBuilder builds chunks for multiple different aggregates.
type aggrChunkBuilder struct {
	mint, maxt int64
	added      int

	chunks [5]chunkenc.Chunk
	apps   [5]chunkenc.Appender
}

func (b *aggrChunkBuilder) add(t int64, aggr *aggregator) {
	if t < b.mint {
		b.mint = t
	}
	if t > b.maxt {
		b.maxt = t
	}
	b.apps[downsample.AggrSum].Append(t, aggr.sum)
	b.apps[downsample.AggrMin].Append(t, aggr.min)
	b.apps[downsample.AggrMax].Append(t, aggr.max)
	b.apps[downsample.AggrCount].Append(t, float64(aggr.count))
	b.apps[downsample.AggrCounter].Append(t, aggr.counter)

	b.added++
}

func (b *aggrChunkBuilder) encode() chunks.Meta {
	return chunks.Meta{
		MinTime: b.mint,
		MaxTime: b.maxt,
		Chunk:   downsample.EncodeAggrChunk(b.chunks),
	}
}

// downsampleAggr downsamples a sequence of aggregation chunks to the given resolution.
func downsampleAggr(logger log.Logger, chks []*downsample.AggrChunk, buf *[]sample, mint, maxt, inRes, outRes int64) ([]chunks.Meta, error) {
	var numSamples int
	for _, c := range chks {
		numSamples += c.NumSamples()
	}
	numChunks := targetChunkCount(mint, maxt, inRes, outRes, numSamples)
	if numChunks > len(chks) {
		numChunks = len(chks)
	}
	return downsampleAggrLoop(logger, chks, buf, outRes, numChunks)
}

// targetChunkCount calculates how many chunks should be produced when downsampling a series.
// It consider the total time range, the number of input sample, the input and output resolution.
func targetChunkCount(mint, maxt, inRes, outRes int64, count int) (x int) {
	// We compute how many samples we could produce for the given time range and adjust
	// it by how densely the range is actually filled given the number of input samples and their
	// resolution.
	maxSamples := float64((maxt - mint) / outRes)
	expSamples := int(maxSamples*rangeFullness(mint, maxt, inRes, count)) + 1

	// Increase the number of target chunks until each chunk will have less than
	// 140 samples on average.
	for x = 1; expSamples/x > 140; x++ {
	}
	return x
}

// rangeFullness returns the fraction of how the range [mint, maxt] covered
// with count samples at the given step size.
// It return value is bounded to [0, 1].
func rangeFullness(mint, maxt, step int64, count int) float64 {
	f := float64(count) / (float64(maxt-mint) / float64(step))
	if f > 1 {
		return 1
	}
	return f
}

func downsampleAggrLoop(logger log.Logger, chks []*downsample.AggrChunk, buf *[]sample, resolution int64, numChunks int) ([]chunks.Meta, error) {
	// We downsample aggregates only along chunk boundaries. This is required
	// for counters to be downsampled correctly since a chunk's first and last
	// counter values are the true values of the original series. We need
	// to preserve them even across multiple aggregation iterations.
	res := make([]chunks.Meta, 0, numChunks)
	batchSize := len(chks) / numChunks

	for len(chks) > 0 {
		j := batchSize
		if j > len(chks) {
			j = len(chks)
		}
		part := chks[:j]
		chks = chks[j:]

		goodPart := getGoodChunks(logger, part)

		if len(goodPart) == 0 {
			level.Warn(logger).Log("msg", "Skip chunk batch")
			continue
		}

		chk, err := downsampleAggrBatch(goodPart, buf, resolution)
		if err != nil {
			return nil, err
		}
		res = append(res, chk)
	}

	return res, nil
}

func downsampleAggrBatch(chks []*downsample.AggrChunk, buf *[]sample, resolution int64) (chk chunks.Meta, err error) {
	ab := &aggrChunkBuilder{}
	mint, maxt := int64(math.MaxInt64), int64(math.MinInt64)
	var reuseIt chunkenc.Iterator

	// do does a generic aggregation for count, sum, min, and max aggregates.
	// Counters need special treatment.
	do := func(at downsample.AggrType, f func(a *aggregator) float64) error {
		*buf = (*buf)[:0]
		// Expand all samples for the aggregate type.
		for _, chk := range chks {
			c, err := chk.Get(at)
			if err == downsample.ErrAggrNotExist {
				continue
			} else if err != nil {
				return err
			}
			if err := expandChunkIterator(c.Iterator(reuseIt), buf); err != nil {
				return err
			}
		}
		if len(*buf) == 0 {
			return nil
		}
		ab.chunks[at] = chunkenc.NewXORChunk()
		ab.apps[at], _ = ab.chunks[at].Appender()

		downsampleBatch(*buf, resolution, func(t int64, a *aggregator) {
			if t < mint {
				mint = t
			} else if t > maxt {
				maxt = t
			}
			ab.apps[at].Append(t, f(a))
		})
		return nil
	}
	if err := do(downsample.AggrCount, func(a *aggregator) float64 {
		// To get correct count of elements from already downsampled count chunk
		// we have to sum those values.
		return a.sum
	}); err != nil {
		return chk, err
	}
	if err = do(downsample.AggrSum, func(a *aggregator) float64 {
		return a.sum
	}); err != nil {
		return chk, err
	}
	if err := do(downsample.AggrMin, func(a *aggregator) float64 {
		return a.min
	}); err != nil {
		return chk, err
	}
	if err := do(downsample.AggrMax, func(a *aggregator) float64 {
		return a.max
	}); err != nil {
		return chk, err
	}

	// Handle counters by applying resets directly.
	acs := make([]chunkenc.Iterator, 0, len(chks))
	for _, achk := range chks {
		c, err := achk.Get(downsample.AggrCounter)
		if err == downsample.ErrAggrNotExist {
			continue
		} else if err != nil {
			return chk, err
		}
		acs = append(acs, c.Iterator(reuseIt))
	}
	*buf = (*buf)[:0]
	it := downsample.NewApplyCounterResetsIterator(acs...)

	if err := expandChunkIterator(it, buf); err != nil {
		return chk, err
	}
	if len(*buf) == 0 {
		ab.mint = mint
		ab.maxt = maxt
		return ab.encode(), nil
	}
	ab.chunks[downsample.AggrCounter] = chunkenc.NewXORChunk()
	ab.apps[downsample.AggrCounter], _ = ab.chunks[downsample.AggrCounter].Appender()

	// Retain first raw value; see ApplyCounterResetsSeriesIterator.
	ab.apps[downsample.AggrCounter].Append((*buf)[0].t, (*buf)[0].v)

	lastT := downsampleBatch(*buf, resolution, func(t int64, a *aggregator) {
		if t < mint {
			mint = t
		} else if t > maxt {
			maxt = t
		}
		ab.apps[downsample.AggrCounter].Append(t, a.counter)
	})

	// Retain last raw value; see ApplyCounterResetsSeriesIterator.
	ab.apps[downsample.AggrCounter].Append(lastT, it.LastV())

	ab.mint = mint
	ab.maxt = maxt
	return ab.encode(), nil
}

// expandChunkIterator reads all samples from the iterator and appends them to buf.
// Stale markers and out of order samples are skipped.
func expandChunkIterator(it chunkenc.Iterator, buf *[]sample) error {
	// For safety reasons, we check for each sample that it does not go back in time.
	// If it does, we skip it.
	lastT := int64(0)

	for it.Next() != chunkenc.ValNone {
		t, v := it.At()
		if value.IsStaleNaN(v) {
			continue
		}
		if t >= lastT {
			*buf = append(*buf, sample{t, v})
			lastT = t
		}
	}
	return it.Err()
}

// end of copied code //
//⬆️⬆️⬆️⬆️⬆️⬆️⬆️⬆️⬆️ //
