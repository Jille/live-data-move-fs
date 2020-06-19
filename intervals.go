package ldmfs

type interval struct {
	s, e int64
}

type Intervals struct {
	intervals []interval
}

func (i *Intervals) Add(s, e int64) {
	for j, iv := range i.intervals {
		if (iv.s <= s && s <= iv.e) || (iv.s <= e && e <= iv.e) {
			i.intervals[j].s = min(iv.s, s)
			i.intervals[j].e = max(iv.e, e)
			return
		}
		if iv.e < s {
			i.intervals = insert(i.intervals, j, interval{s, e})
			return
		}
	}
	i.intervals = append(i.intervals, interval{s, e})
}

func (i *Intervals) Del(s, e int64) {
	for j := len(i.intervals) - 1; j >= 0; j-- {
		iv := i.intervals[j]
		if iv.e < s {
			return
		}
		if s <= iv.s && iv.e <= e {
			i.intervals = remove(i.intervals, j)
		} else if iv.s <= s && e <= iv.e {
			i.intervals[j].e = s
			i.intervals = insert(i.intervals, j+1, interval{e, iv.e})
		} else if iv.s <= e && e <= iv.e {
			i.intervals[j].s = e
		} else if iv.s <= s && s <= iv.e {
			i.intervals[j].e = s
		}
	}
}

func (i *Intervals) Has(s, e int64) bool {
	for _, iv := range i.intervals {
		if s < iv.e {
			return (iv.s <= s && e <= iv.e)
		}
	}
	return false
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// TODO(quis): Make these two functions in-place.

func insert(intervals []interval, i int, iv interval) []interval {
	ret := make([]interval, 0, len(intervals)+1)
	ret = append(ret, intervals[:i]...)
	ret = append(ret, iv)
	ret = append(ret, intervals[i:]...)
	return ret
}

func remove(intervals []interval, i int) []interval {
	ret := make([]interval, 0, len(intervals)-1)
	ret = append(ret, intervals[:i]...)
	ret = append(ret, intervals[i+1:]...)
	return ret
}
