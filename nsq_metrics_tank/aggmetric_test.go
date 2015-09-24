package main

import (
	"fmt"
	"testing"
)

type point struct {
	ts  uint32
	val float64
}

func (p point) String() string {
	return fmt.Sprintf("point{%0.f at %d}", p.val, p.ts)
}

type Checker struct {
	t      *testing.T
	agg    *AggMetric
	points []point
}

func NewChecker(t *testing.T, agg *AggMetric) *Checker {
	return &Checker{t, agg, make([]point, 0)}
}

// always add points in ascending order, never same ts!
func (c *Checker) Add(ts uint32, val float64) {
	c.agg.Add(ts, val)
	c.points = append(c.points, point{ts, val})
}

// from to is the range that gets fed to AggMetric
// first/last is what we use as data range to compare to
// these may be different because AggMetric returns broader rangers (due to packed format),
func (c *Checker) Verify(from, to, first, last uint32) {
	iters := c.agg.GetSafe(from, to)
	// we don't do checking or fancy logic, it is assumed that the caller made sure first and last are ts of actual points
	var pi int // index of first point we want
	var pj int // index of last point we want
	for pi = 0; c.points[pi].ts != first; pi++ {
	}
	for pj = pi; c.points[pj].ts != last; pj++ {
	}
	c.t.Logf("cmp AggMetric.GetSafe(%d,%d) to %d <= ts <= %d, i.e. p[%d]=%s until p[%d]=%s (inclusive)", from, to, first, last, pi, c.points[pi], pj, c.points[pj])
	index := pi - 1
	for _, iter := range iters {
		for iter.Next() {
			index++
			tt, vv := iter.Values()
			if index > pj {
				c.t.Fatalf("Values()=(%v,%v), want end of stream\n", tt, vv)
			}
			if c.points[index].ts != tt || c.points[index].val != vv {
				c.t.Fatalf("Values()=(%v,%v), want (%v,%v)\n", tt, vv, c.points[index].ts, c.points[index].val)
			}
		}
	}
	if index != pj {
		c.t.Fatalf("not all values returned. missing %v", c.points[index:pj+1])
	}
}

func TestAggMetric(t *testing.T) {
	c := NewChecker(t, NewAggMetric("foo", 100, 5))

	// basic case, single range
	c.Add(101, 101)
	c.Verify(100, 200, 101, 101)
	c.Add(105, 105)
	c.Verify(100, 199, 101, 105)
	c.Add(115, 115)
	c.Add(125, 125)
	c.Add(135, 135)
	c.Verify(100, 199, 101, 135)

	// add new ranges, aligned and unaligned
	c.Add(200, 200)
	c.Add(315, 315)
	c.Verify(100, 399, 101, 315)

	// get subranges
	c.Verify(120, 299, 101, 200)
	c.Verify(220, 299, 200, 200)
	c.Verify(312, 330, 315, 315)

	// border dancing. good for testing inclusivity and exclusivity
	c.Verify(100, 199, 101, 135)
	c.Verify(100, 200, 101, 135)
	c.Verify(100, 201, 101, 200)
	c.Verify(198, 199, 101, 135)
	c.Verify(199, 200, 101, 135)
	c.Verify(200, 201, 200, 200)
	c.Verify(201, 202, 200, 200)
	c.Verify(299, 300, 200, 200)
	c.Verify(300, 301, 315, 315)

	// skipping
	c.Add(510, 510)
	c.Add(512, 512)
	c.Verify(100, 599, 101, 512)

	// basic wraparound
	c.Add(610, 610)
	c.Add(612, 612)
	c.Add(710, 710)
	c.Add(712, 712)
	// TODO would be nice to test that it panics when requesting old range. something with recover?
	//c.Verify(100, 799, 101, 512)

	// largest range we have so far
	c.Verify(300, 799, 315, 712)
	// a smaller range
	c.Verify(502, 799, 510, 712)

	// the circular buffer had these ranges:
	// 100 200 300 skipped 500
	// then we made it:
	// 600 700 300 skipped 500
	// now we want to do another wrap around with skip (must have cleared old data)
	// let's jump to 1200. the accessible range should then be 800-1200
	// clea 1200 clea clea clea
	// we can't (and shouldn't, due to abstraction) test the clearing itself
	// but we just check we only get this point
	c.Add(1299, 1299)
	c.Verify(800, 1299, 1299, 1299)
}
