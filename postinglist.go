package tinysearch

import (
	"container/list"
)

type DocID int64

func NewPostingList() *PostingList {
	return &PostingList{index: make(map[DocID]*list.Element)}
}

type PostingList struct {
	index map[DocID]*list.Element
	list  list.List
}

// Add adds the given DocID to the end of the posting list. Noop if the given DocID is already in the
// posting list.
func (pl *PostingList) Add(docID DocID) {
	if _, ok := pl.index[docID]; !ok {
		pl.index[docID] = pl.list.PushBack(docID)
	}
}

// Remove removes the given element from the list. Noop if the element was not already present.
// Returns true if the list is empty.
func (pl *PostingList) Remove(docID DocID) bool {
	if node, ok := pl.index[docID]; ok {
		pl.list.Remove(node)
	}
	return pl.list.Front() == nil
}

func (pl *PostingList) Iterator() PostingListIterator {
	if pl == nil {
		return EmptyIterator
	}

	if front := pl.list.Front(); front == nil {
		return EmptyIterator
	} else {
		return &listIterator{
			Element:  front,
			elements: pl.list.Len(),
		}
	}
}
