package tinysearch

import (
	"container/list"
)

type PostingListIterator interface {
	Len() int
	Peek() (DocID, bool)
	Next()
}

func Iterate(iterator PostingListIterator, f func(docID DocID) bool) {
	for {
		docID, ok := iterator.Peek()
		if !ok || !f(docID) {
			return
		}

		iterator.Next()
	}
}

var _ PostingListIterator = (*listIterator)(nil)

var EmptyIterator PostingListIterator = new(listIterator)

type listIterator struct {
	*list.Element
	elements int
}

func (p *listIterator) Peek() (DocID, bool) {
	if p.Element == nil {
		return 0, false
	} else {
		return p.Element.Value.(DocID), true
	}
}

func (p *listIterator) Next() {
	if p.Element != nil {
		p.Element = p.Element.Next()
	}
}

func (p *listIterator) Len() int {
	return p.elements
}

var _ PostingListIterator = (*andIterator)(nil)

func NewAndIterator(left, right PostingListIterator) PostingListIterator {
	a := &andIterator{left, right}
	// a.next must be called at least once before Peek because it maintains the invariant that a.left and
	// a.right point to the same DocID, or that either one is exhausted.
	a.next()
	return a
}

type andIterator struct {
	left, right PostingListIterator
}

func (a *andIterator) peek() (left, right DocID, ok bool) {
	if left, ok = a.left.Peek(); !ok {
		return 0, 0, false
	}

	if right, ok = a.right.Peek(); !ok {
		return 0, 0, false
	}

	return left, right, true
}

func (a *andIterator) Peek() (DocID, bool) {
	left, _, ok := a.peek()
	return left, ok
}

func (a *andIterator) Next() {
	a.left.Next()
	a.next()
}

func (a *andIterator) next() {
	for {
		left, right, ok := a.peek()
		if !ok {
			return
		}

		switch {
		case left == right:
			return
		case left < right:
			a.left.Next()
		default:
			a.right.Next()
		}
	}
}

func (a *andIterator) Len() int {
	return min(a.left.Len(), a.right.Len())
}

var _ PostingListIterator = (*orIterator)(nil)

func NewOrIterator(left, right PostingListIterator) PostingListIterator {
	return &orIterator{left, right}
}

type orIterator struct {
	left, right PostingListIterator
}

func (o *orIterator) Peek() (DocID, bool) {
	left, leftOk := o.left.Peek()
	right, rightOk := o.right.Peek()
	switch {
	case leftOk && rightOk:
		return min(left, right), true
	case leftOk:
		return left, true
	case rightOk:
		return right, true
	default:
		return 0, false
	}
}

func (o *orIterator) Next() {
	left, leftOk := o.left.Peek()
	right, rightOk := o.right.Peek()

	switch {
	case leftOk && rightOk:
		switch {
		case left == right:
			o.left.Next()
			o.right.Next()
		case left < right:
			o.left.Next()
		case left > right:
			o.right.Next()
		}
	case leftOk:
		o.left.Next()
	case rightOk:
		o.right.Next()
	case !(leftOk || rightOk):
		return
	}
}

func (o *orIterator) Len() int {
	return max(o.left.Len(), o.right.Len())
}

func NewNotIterator(positive, negative PostingListIterator) PostingListIterator {
	n := &notIterator{positive, negative}
	// n.next must be called before Peek because it maintains the invariant that n.positive points to a
	// DocID which is less than the DocID n.negative points to (or that n.negative is exhausted).
	n.next()
	return n
}

type notIterator struct {
	positive, negative PostingListIterator
}

func (n *notIterator) Len() int {
	return n.positive.Len()
}

func (n *notIterator) Peek() (DocID, bool) {
	return n.positive.Peek()
}

func (n *notIterator) Next() {
	n.positive.Next()
	n.next()
}

func (n *notIterator) next() {
	for {
		positive, ok := n.positive.Peek()
		if !ok {
			return
		}
		negative, ok := n.negative.Peek()

		switch {
		case !ok || positive < negative:
			return
		case positive == negative:
			n.positive.Next()
			n.negative.Next()
		case positive > negative:
			n.negative.Next()
		}
	}
}
