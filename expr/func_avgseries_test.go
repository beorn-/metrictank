package expr

import (
	"math"
	"testing"

	"github.com/raintank/metrictank/api/models"
)

func TestAvgSeriesIdentity(t *testing.T) {
	testAvgSeries(
		"identity",
		[][]models.Series{
			{
				{
					QueryPatt:  "single",
					Datapoints: getCopy(a),
				},
			},
		},
		models.Series{
			Target:     "averageSeries(single)",
			Datapoints: getCopy(a),
		},
		t,
	)
}
func TestAvgSeriesQueryToSingle(t *testing.T) {
	testAvgSeries(
		"query-to-single",
		[][]models.Series{
			{
				{
					QueryPatt:  "foo.*",
					Datapoints: getCopy(a),
				},
			},
		},
		models.Series{
			Target:     "averageSeries(foo.*)",
			Datapoints: getCopy(a),
		},
		t,
	)
}
func TestAvgSeriesMultiple(t *testing.T) {
	testAvgSeries(
		"avg-multiple-series",
		[][]models.Series{
			{
				{
					QueryPatt:  "foo.*",
					Datapoints: getCopy(a),
				},
				{
					QueryPatt:  "foo.*",
					Datapoints: getCopy(b),
				},
			},
		},
		models.Series{
			Target:     "averageSeries(foo.*)",
			Datapoints: getCopy(avgab),
		},
		t,
	)
}
func TestAvgSeriesMultipleDiffQuery(t *testing.T) {
	testAvgSeries(
		"avg-multiple-serieslists",
		[][]models.Series{
			{
				{
					QueryPatt:  "foo.*",
					Datapoints: getCopy(a),
				},
				{
					QueryPatt:  "foo.*",
					Datapoints: getCopy(b),
				},
			},
			{
				{
					QueryPatt:  "movingAverage(bar, '1min')",
					Datapoints: getCopy(c),
				},
			},
		},
		models.Series{
			Target:     "averageSeries(foo.*,movingAverage(bar, '1min'))",
			Datapoints: getCopy(avgabc),
		},
		t,
	)
}

func testAvgSeries(name string, in [][]models.Series, out models.Series, t *testing.T) {
	f := NewAvgSeries()
	var input []interface{}
	for _, i := range in {
		input = append(input, i)
	}
	got, err := f.Exec(make(map[Req][]models.Series), input...)
	if err != nil {
		t.Fatalf("case %q: err should be nil. got %q", name, err)
	}
	if len(got) != 1 {
		t.Fatalf("case %q: avgSeries output should be only 1 thing (a series) not %d", name, len(got))
	}
	g, ok := got[0].(models.Series)
	if !ok {
		t.Fatalf("case %q: expected avg output of models.Series type", name)
	}
	if g.Target != out.Target {
		t.Fatalf("case %q: expected target %q, got %q", name, out.Target, g.Target)
	}
	if len(g.Datapoints) != len(out.Datapoints) {
		t.Fatalf("case %q: len output expected %d, got %d", name, len(out.Datapoints), len(g.Datapoints))
	}
	for j, p := range g.Datapoints {
		bothNaN := math.IsNaN(p.Val) && math.IsNaN(out.Datapoints[j].Val)
		if (bothNaN || p.Val == out.Datapoints[j].Val) && p.Ts == out.Datapoints[j].Ts {
			continue
		}
		t.Fatalf("case %q: output point %d - expected %v got %v", name, j, out.Datapoints[j], p)
	}
}