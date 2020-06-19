package ldmfs

import (
	"testing"
	"reflect"
)

func TestIntervals(t *testing.T) {
	tests := []struct{
		add []interval
		del []interval
		has []interval
		notHas []interval
		want Intervals
	}{
		{
			add: []interval{{3, 5}},
			want: Intervals{[]interval{{3, 5}}},
			has: []interval{{3, 5}, {3, 4}, {4, 5}},
			notHas: []interval{{2, 5}, {2, 3}, {5, 5}, {5, 6}, {6, 7}, {1, 7}},
		},
		{
			add: []interval{{3, 4}, {4, 5}},
			want: Intervals{[]interval{{3, 5}}},
			has: []interval{{3, 5}, {3, 4}, {4, 5}},
			notHas: []interval{{2, 5}, {2, 3}, {5, 5}, {5, 6}, {6, 7}, {1, 7}},
		},
		{
			add: []interval{{3, 10}},
			del: []interval{{5, 100}},
			want: Intervals{[]interval{{3, 5}}},
			has: []interval{{3, 5}, {3, 4}, {4, 5}},
			notHas: []interval{{2, 5}, {2, 3}, {5, 5}, {5, 6}, {6, 7}, {1, 7}},
		},
		{
			add: []interval{{3, 10}},
			del: []interval{{3, 100}},
			want: Intervals{[]interval{}},
		},
		{
			add: []interval{{3, 10}},
			del: []interval{{2, 100}},
			want: Intervals{[]interval{}},
		},
		{
			add: []interval{{3, 10}},
			del: []interval{{5, 6}},
			want: Intervals{[]interval{{3, 5}, {6, 10}}},
			has: []interval{{3, 4}, {6, 7}},
			notHas: []interval{{5, 6}, {3, 10}},
		},
	}
	for _, tc := range tests {
		i := Intervals{}
		for _, iv := range tc.add {
			i.Add(iv.s, iv.e)
		}
		for _, iv := range tc.del {
			i.Del(iv.s, iv.e)
		}
		if !reflect.DeepEqual(i, tc.want) {
			t.Errorf("%v is not %v", i, tc.want)
		}
		for _, iv := range tc.has {
			if !i.Has(iv.s, iv.e) {
				t.Errorf("%v doesn't have [%d, %d]", i, iv.s, iv.e)
			}
		}
		for _, iv := range tc.notHas {
			if i.Has(iv.s, iv.e) {
				t.Errorf("%v has [%d, %d]", i, iv.s, iv.e)
			}
		}
	}
}
