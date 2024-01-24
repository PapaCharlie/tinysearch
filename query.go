package tinysearch

type QueryBuilder[DOCUMENT, FIELD comparable] func(FIELD) Query[DOCUMENT]

type Query[DOCUMENT comparable] interface {
	iterator(index *InvertedIndex[DOCUMENT]) PostingListIterator
}

type queryFunc[DOCUMENT comparable] func(index *InvertedIndex[DOCUMENT]) PostingListIterator

func (q queryFunc[DOCUMENT]) iterator(index *InvertedIndex[DOCUMENT]) PostingListIterator {
	return q(index)
}

func NewAndQuery[DOCUMENT comparable](ands ...Query[DOCUMENT]) Query[DOCUMENT] {
	return AndQuery[DOCUMENT](ands)
}

type AndQuery[DOCUMENT comparable] []Query[DOCUMENT]

func (a AndQuery[DOCUMENT]) iterator(index *InvertedIndex[DOCUMENT]) PostingListIterator {
	if len(a) == 0 {
		return EmptyIterator
	}

	iterators := make([]PostingListIterator, len(a))
	for i, q := range a {
		iterators[i] = q.iterator(index)
	}

	iterator := iterators[0]
	for _, itr := range iterators[1:] {
		iterator = NewAndIterator(iterator, itr)
	}

	return iterator
}

func NewOrQuery[DOCUMENT comparable](ors ...Query[DOCUMENT]) Query[DOCUMENT] {
	return OrQuery[DOCUMENT](ors)
}

type OrQuery[DOCUMENT comparable] []Query[DOCUMENT]

func (o OrQuery[DOCUMENT]) iterator(index *InvertedIndex[DOCUMENT]) PostingListIterator {
	if len(o) == 0 {
		return EmptyIterator
	}

	iterator := o[0].iterator(index)
	for _, q := range o[1:] {
		iterator = NewOrIterator(
			iterator,
			q.iterator(index),
		)
	}

	return iterator
}

type notQuery[DOCUMENT comparable] struct {
	positive, negative Query[DOCUMENT]
}

func (n *notQuery[DOCUMENT]) iterator(index *InvertedIndex[DOCUMENT]) PostingListIterator {
	return NewNotIterator(
		n.positive.iterator(index),
		n.negative.iterator(index),
	)
}

func NewNotQuery[DOCUMENT comparable](positive, negative Query[DOCUMENT]) Query[DOCUMENT] {
	return &notQuery[DOCUMENT]{
		positive: positive,
		negative: negative,
	}
}
