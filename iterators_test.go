package tinysearch

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAndOrIterators(t *testing.T) {
	tests := []struct {
		name  string
		lists [][]DocID
		not   []DocID
	}{
		{
			name:  "both empty",
			lists: [][]DocID{nil, nil},
		},
		{
			name:  "left empty",
			lists: [][]DocID{nil, {1}},
			not:   []DocID{1},
		},
		{
			name:  "right empty",
			lists: [][]DocID{{1}, nil},
			not:   []DocID{1},
		},
		{
			name:  "no overlap left",
			lists: [][]DocID{{1, 3, 5}, {2, 4}},
			not:   []DocID{3},
		},
		{
			name:  "no overlap right",
			lists: [][]DocID{{2, 4}, {1, 3, 5}},
			not:   []DocID{1, 2},
		},
		{
			name:  "sample 1",
			lists: [][]DocID{{1, 4}, {1, 3, 5}},
			not:   []DocID{3, 5},
		},
		{
			name:  "sample 2",
			lists: [][]DocID{{4, 5}, {1, 3, 5}},
		},
		{
			name:  "sample 3",
			lists: [][]DocID{{1, 4, 5}, {1, 3, 5}},
		},
		{
			name:  "sample 4",
			lists: [][]DocID{{2, 3, 6}, {1, 3, 5}},
		},
		{
			name:  "sample 5",
			lists: [][]DocID{{1, 2, 3, 4, 5}, {1, 2, 3, 4, 5}},
		},
		{
			name:  "sample 6",
			lists: [][]DocID{{0, 1, 2, 3, 7, 8, 9, 11, 13, 14, 16, 19, 21}, {2, 11, 14, 15, 16, 22}},
			not:   []DocID{2, 11},
		},
		{
			name:  "sample 7",
			lists: [][]DocID{{1, 3, 5}, {2, 4, 6}},
			not:   []DocID{1, 2, 3, 4, 5, 6},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Run("and", func(t *testing.T) {
				testAnd(t, test.lists, test.not)
			})
			t.Run("or", func(t *testing.T) {
				testOr(t, test.lists, test.not)
			})
		})
	}
}

func testAnd(t *testing.T, lists [][]DocID, not []DocID) {
	newAndIterator := func() PostingListIterator {
		itr := newIterator(lists[0])
		for _, l := range lists[1:] {
			itr = NewAndIterator(itr, newIterator(l))
		}
		return itr
	}

	var expected []DocID
	for docID, count := range docCounts(lists) {
		if count == len(lists) {
			expected = append(expected, docID)
		}
	}
	slices.Sort(expected)

	require.Equal(t, expected, iteratorValues(newAndIterator()), lists)

	testNot(t, expected, newAndIterator(), not)
}

func testOr(t *testing.T, lists [][]DocID, not []DocID) {
	newOrIterator := func() PostingListIterator {
		itr := newIterator(lists[0])
		for _, l := range lists[1:] {
			itr = NewOrIterator(
				itr,
				newIterator(l),
			)
		}
		return itr
	}

	var expected []DocID
	for docID := range docCounts(lists) {
		expected = append(expected, docID)
	}
	slices.Sort(expected)

	require.Equal(t, expected, iteratorValues(newOrIterator()))

	testNot(t, expected, newOrIterator(), not)
}

func testNot(t *testing.T, expected []DocID, itr PostingListIterator, not []DocID) {
	notMap := make(map[DocID]bool, len(not))
	for _, docId := range not {
		notMap[docId] = true
	}

	expected = slices.DeleteFunc(expected, func(id DocID) bool { return notMap[id] })
	if len(expected) == 0 {
		expected = nil
	}
	actual := iteratorValues(NewNotIterator(itr, newIterator(not)))

	require.Equal(t, expected, actual)
}

func docCounts(lists [][]DocID) map[DocID]int {
	counts := make(map[DocID]int)
	for _, l := range lists {
		for _, docID := range l {
			counts[docID]++
		}
	}
	return counts
}

func TestNotIterator(t *testing.T) {
	tests := []struct {
		name     string
		positive []DocID
		negative []DocID
		expected []DocID
	}{
		{
			name:     "empty positive",
			positive: nil,
			negative: []DocID{1},
			expected: nil,
		},
		{
			name:     "empty negative",
			positive: []DocID{1},
			negative: nil,
			expected: []DocID{1},
		},
		{
			name:     "left overlap",
			positive: []DocID{1, 2, 3, 4},
			negative: []DocID{1, 2},
			expected: []DocID{3, 4},
		},
		{
			name:     "right overlap",
			positive: []DocID{1, 2, 3, 4},
			negative: []DocID{3, 4},
			expected: []DocID{1, 2},
		},
		{
			name:     "edge overlap",
			positive: []DocID{1, 2, 3, 4},
			negative: []DocID{1, 4},
			expected: []DocID{2, 3},
		},
		{
			name:     "multi overlap",
			positive: []DocID{1, 2, 3, 4, 5},
			negative: []DocID{1, 3, 5},
			expected: []DocID{2, 4},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(
				t,
				test.expected,
				iteratorValues(NewNotIterator(newIterator(test.positive), newIterator(test.negative))),
			)
		})
	}
}

func TestStackedIterator(t *testing.T) {
	tests := []struct {
		name     string
		iterator PostingListIterator
		expected []DocID
	}{
		{
			name: "normal",
			iterator: NewAndIterator(
				NewOrIterator(
					newPostingList(1, 3, 4).Iterator(),
					newPostingList(1, 2, 3).Iterator(),
				),
				newPostingList(1, 3).Iterator(),
			),
			expected: []DocID{1, 3},
		},
		{
			name: "normal inverted",
			iterator: NewAndIterator(
				newPostingList(1, 3).Iterator(),
				NewOrIterator(
					newPostingList(1, 3, 4).Iterator(),
					newPostingList(1, 2, 3).Iterator(),
				),
			),
			expected: []DocID{1, 3},
		},
		{
			name: "empty and",
			iterator: NewAndIterator(
				newPostingList().Iterator(),
				NewOrIterator(
					newPostingList(1, 3, 4).Iterator(),
					newPostingList(1, 2, 3).Iterator(),
				),
			),
			expected: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, iteratorValues(test.iterator))
		})
	}

}

func TestIteratorFuzzed(t *testing.T) {
	const docCount = 100
	docIDs := make([]DocID, docCount)
	for i := DocID(0); i < docCount; i++ {
		docIDs[i] = i
	}

	rng := rand.New(rand.NewSource(0))

	testMerge := func(t *testing.T, docIDs []DocID, testCount, listCount int64) {
		lists := make([][]DocID, listCount)

		for i := range lists {
			rng.Shuffle(len(docIDs), func(i, j int) { docIDs[i], docIDs[j] = docIDs[j], docIDs[i] })
			lists[i] = slices.Clone(docIDs[:rng.Intn(docCount)])
		}

		not := slices.Clone(docIDs[:rng.Intn(docCount)])

		testAnd(t, lists, not)
		testOr(t, lists, not)
	}

	for listCount := int64(1); listCount <= 5; listCount++ {
		t.Run(fmt.Sprintf("listCount=%d", listCount), func(t *testing.T) {
			for testCount := int64(0); testCount < 1000; testCount++ {
				testMerge(t, docIDs, testCount, listCount)
			}
		})
	}
}

func TestIterator(t *testing.T) {
	itr := newPostingList().Iterator()
	docID, ok := itr.Peek()
	require.Equal(t, DocID(0), docID)
	require.False(t, ok)

	expected := []DocID{1, 2, 3, 4, 5}
	itr = newPostingList(expected...).Iterator()
	require.Equal(t, expected, iteratorValues(itr))
}

func newPostingList(docIDs ...DocID) *PostingList {
	l := NewPostingList()
	slices.Sort(docIDs)
	for _, docID := range docIDs {
		l.Add(docID)
	}
	return l
}

func iteratorValues(itr PostingListIterator) (out []DocID) {
	Iterate(itr, func(docID DocID) bool {
		out = append(out, docID)
		return true
	})
	return out
}

func newIterator(docs []DocID) PostingListIterator {
	slices.Sort(docs)
	return &docSlice{docs: docs}
}

type docSlice struct {
	docs []DocID
	idx  int
}

func (d *docSlice) Len() int {
	return len(d.docs)
}

func (d *docSlice) Peek() (DocID, bool) {
	if d.idx >= len(d.docs) {
		return 0, false
	} else {
		return d.docs[d.idx], true
	}
}

func (d *docSlice) Next() {
	d.idx++
}
