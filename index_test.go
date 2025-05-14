package tinysearch

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInvertedIndex(t *testing.T) {
	type doc struct {
		i int
		s string
	}

	var (
		isEvenField = NewInvertedIndexField[doc, bool](func(doc doc, f func(value bool)) {
			f(doc.i%2 == 0)
		})
		prefix2Field = NewInvertedIndexField[doc, string](func(doc doc, f func(value string)) {
			f(doc.s[:2])
		})
		runeField = NewInvertedIndexField[doc, rune](func(doc doc, f func(value rune)) {
			for _, r := range doc.s {
				f(r)
			}
		})
	)

	idx := NewInvertedIndex[doc](isEvenField, prefix2Field, runeField)
	roo := doc{
		i: 0,
		s: "roo",
	}
	require.True(t, idx.Add(roo))
	require.False(t, idx.Add(roo))

	bar := doc{
		i: 1,
		s: "bar",
	}
	require.True(t, idx.Add(bar))
	baz := doc{
		i: 2,
		s: "baz",
	}
	require.True(t, idx.Add(baz))

	require.Equal(t, []doc{roo, baz}, query(idx, isEvenField.Query(true)))
	require.Equal(t, []doc{bar, baz}, query(idx, prefix2Field.Query("ba")))
	require.Equal(t, []doc{roo, bar}, query(idx, runeField.Query('r')))
	rooBarQuery := NewOrQuery(prefix2Field.Query("ro"), isEvenField.Query(false))
	require.Equal(t, []doc{roo, bar}, query(idx, rooBarQuery))
	require.Equal(t, []doc{roo}, query(
		idx,
		NewNotQuery[doc](rooBarQuery, prefix2Field.Query("ba")),
	))

	bazQuery := NewAndQuery(isEvenField.Query(true), runeField.Query('z'))
	require.Equal(t, []doc{baz}, query(idx, bazQuery))
	require.Equal(t, []doc(nil), query(
		idx,
		NewNotQuery[doc](bazQuery, prefix2Field.Query("ba")),
	))

	require.True(t, idx.Remove(roo))
	require.False(t, idx.Remove(roo))

	require.Equal(t, []doc{baz}, query(idx, isEvenField.Query(true)))
	require.Equal(t, []doc{bar, baz}, query(idx, prefix2Field.Query("ba")))
	require.Equal(t, []doc{bar}, query(idx, runeField.Query('r')))
	require.Equal(t, []doc{bar}, query(idx, rooBarQuery))
	require.Equal(t, []doc(nil), query(
		idx,
		NewNotQuery[doc](rooBarQuery, prefix2Field.Query("ba")),
	))

	require.Equal(t, []doc{baz}, query(idx, bazQuery))
	require.Equal(t, []doc(nil), query(
		idx,
		NewNotQuery[doc](bazQuery, prefix2Field.Query("ba")),
	))

	// Test empty queries
	require.Equal(t, []doc(nil), query(idx, OrQuery[doc]{}))
	require.Equal(t, []doc(nil), query(idx, AndQuery[doc]{}))
}

func query[DOCUMENT comparable](idx *InvertedIndex[DOCUMENT], query Query[DOCUMENT]) []DOCUMENT {
	return slices.Collect(idx.Query(query))
}
