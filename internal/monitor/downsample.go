package monitor

// BrailleCellWidth is the number of horizontal dot columns in one braille
// character (2x4 dot grid). It defines the chart's horizontal resolution.
const BrailleCellWidth = 2

// downsampleBins returns the number of envelope bins for a chart's braille
// resolution.
func downsampleBins(chartWidth int) int {
	if chartWidth < 1 {
		return 1
	}
	return chartWidth * BrailleCellWidth
}

// shouldDownsample reports whether a run of points should be reduced to an
// envelope: roughly 2x the chart's horizontal braille resolution per the
// implementation plan.
func shouldDownsample(points []Point, chartWidth int, thresholdFactor float64) bool {
	if thresholdFactor <= 0 {
		thresholdFactor = 2
	}
	return len(points) > int(thresholdFactor*float64(downsampleBins(chartWidth)))
}

// DownsampleSeries reduces an ordered run of points to a deterministic per-bin
// min/max envelope retaining the first, minimum, maximum, and last points of
// each bin in original order. It never invents points, so gaps, stage breaks,
// and x regressions already decided by the caller are preserved.
func DownsampleSeries(points []Point, chartWidth int) []Point {
	bins := downsampleBins(chartWidth)
	if len(points) <= bins {
		return points
	}
	out := make([]Point, 0, 2*bins)
	for b := 0; b < bins; b++ {
		start, end := binRange(b, bins, len(points))
		if start >= end {
			continue
		}
		minIdx, maxIdx := start, start
		for i := start + 1; i < end; i++ {
			if points[i].Y < points[minIdx].Y {
				minIdx = i
			}
			if points[i].Y > points[maxIdx].Y {
				maxIdx = i
			}
		}
		out = appendEnvelope(out, points, start, minIdx, maxIdx, end-1)
	}
	return out
}

func binRange(b, bins, n int) (int, int) {
	if bins <= 0 {
		bins = 1
	}
	start := b * n / bins
	end := (b + 1) * n / bins
	if end > n {
		end = n
	}
	return start, end
}

// appendEnvelope appends the first/min/max/last points of a bin in original
// order, skipping any candidate already emitted.
func appendEnvelope(out []Point, all []Point, first, minIdx, maxIdx, last int) []Point {
	var prev = -1
	for _, idx := range []int{first, minIdx, maxIdx, last} {
		if idx < 0 || idx >= len(all) || idx == prev {
			continue
		}
		prev = idx
		out = append(out, all[idx])
	}
	return out
}
