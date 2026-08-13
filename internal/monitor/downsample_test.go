package monitor

import (
	"testing"
)

func ypts(ys ...float64) []Point {
	out := make([]Point, len(ys))
	for i, y := range ys {
		out[i] = Point{X: float64(i + 1), Y: y, Seq: int64(i + 1)}
	}
	return out
}

func TestShouldDownsampleThreshold(t *testing.T) {
	// threshold = factor * 2 * chartWidth braille cells; downsample when
	// len strictly exceeds the threshold.
	if shouldDownsample(ypts(1, 2, 3, 4), 1, 2) {
		t.Error("4 points with chartWidth 1 (bins 2, threshold 4) should not downsample")
	}
	if !shouldDownsample(ypts(1, 2, 3, 4, 5), 1, 2) {
		t.Error("5 points with chartWidth 1 (threshold 4) should downsample")
	}
}

func TestDownsampleMonotonicPreservesEndpointsAndOrder(t *testing.T) {
	ys := make([]float64, 40)
	for i := range ys {
		ys[i] = float64(i) * 2
	}
	out := DownsampleSeries(ypts(ys...), 5) // bins = 10
	if len(out) == 0 {
		t.Fatal("empty result")
	}
	if out[0].Y != 0 || out[len(out)-1].Y != 78 {
		t.Errorf("endpoints not preserved: first=%v last=%v", out[0].Y, out[len(out)-1].Y)
	}
	prev := out[0].Y
	for i, p := range out {
		if p.Y < prev {
			t.Fatalf("output not non-decreasing at %d: %v < %v", i, p.Y, prev)
		}
		prev = p.Y
	}
	// envelope should be smaller than the raw 40 points
	if len(out) >= 40 {
		t.Errorf("downsample did not reduce: %d points", len(out))
	}
}

func TestDownsampleSpikePreserved(t *testing.T) {
	ys := make([]float64, 30)
	for i := range ys {
		ys[i] = 1
	}
	ys[15] = 1000 // spike
	out := DownsampleSeries(ypts(ys...), 4)
	found := false
	for _, p := range out {
		if p.Y == 1000 {
			found = true
			break
		}
	}
	if !found {
		t.Error("spike value lost by envelope")
	}
}

func TestDownsampleDuplicateX(t *testing.T) {
	// Multiple samples sharing an x at bin boundaries must stay in order.
	pts := make([]Point, 0, 14)
	x := 1.0
	for i := 0; i < 14; i++ {
		if i%2 == 0 {
			x++
		}
		pts = append(pts, Point{X: x, Y: float64(i), Seq: int64(i + 1)})
	}
	out := DownsampleSeries(pts, 2) // bins = 4
	if len(out) == 0 {
		t.Fatal("empty result")
	}
	if out[0].Seq != 1 || out[len(out)-1].Seq != 14 {
		t.Errorf("boundaries wrong: first seq=%d last seq=%d", out[0].Seq, out[len(out)-1].Seq)
	}
	// Every output point must be one of the originals, in original order.
	seen := map[int64]bool{}
	last := int64(0)
	for _, p := range out {
		if seen[p.Seq] {
			t.Fatalf("duplicate output seq %d", p.Seq)
		}
		seen[p.Seq] = true
		if p.Seq < last {
			t.Fatalf("output out of original order: %d after %d", p.Seq, last)
		}
		last = p.Seq
	}
}

func TestDownsampleGapNotBridged(t *testing.T) {
	// Two clusters far apart in x; the envelope must retain the boundary
	// points in order without inventing intermediate values.
	pts := make([]Point, 0, 20)
	for i := 0; i < 10; i++ {
		pts = append(pts, Point{X: float64(i + 1), Y: 1, Seq: int64(i + 1)})
	}
	for i := 0; i < 10; i++ {
		pts = append(pts, Point{X: 1000 + float64(i), Y: 2, Seq: int64(10 + i + 1)})
	}
	out := DownsampleSeries(pts, 4)
	if len(out) == 0 {
		t.Fatal("empty result")
	}
	// The last point of cluster one (seq 10) and first of cluster two (seq 11)
	// must both be present, adjacent, in order.
	found10, found11 := false, false
	for _, p := range out {
		if p.Seq == 10 {
			found10 = true
		}
		if p.Seq == 11 {
			found11 = true
		}
	}
	if !found10 || !found11 {
		t.Errorf("gap boundaries not retained: seq10=%v seq11=%v", found10, found11)
	}
	// No output y should be a bridged intermediate (only 1 or 2 exist here).
	for _, p := range out {
		if p.Y != 1 && p.Y != 2 {
			t.Errorf("invented y %v", p.Y)
		}
	}
}

func TestDownsampleSmallSeriesUnchanged(t *testing.T) {
	pts := ypts(1, 2, 3)
	out := DownsampleSeries(pts, 10)
	if len(out) != 3 {
		t.Fatalf("small series should pass through unchanged, got %d", len(out))
	}
}
