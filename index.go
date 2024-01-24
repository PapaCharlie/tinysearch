package tinysearch

type InvertedIndex[DOCUMENT comparable] struct {
	fields     map[FieldValueExtractor[DOCUMENT]]any
	docCount   DocID
	docIdToDoc map[DocID]DOCUMENT
	docToDocID map[DOCUMENT]DocID
}

type InvertedIndexField[DOCUMENT, FIELD comparable] struct {
	extractor func(doc DOCUMENT, f func(value FIELD))
}

func (f *InvertedIndexField[DOCUMENT, FIELD]) newMap() any {
	return make(map[FIELD]*PostingList)
}

func (f *InvertedIndexField[DOCUMENT, FIELD]) extractFieldValues(
	idx *InvertedIndex[DOCUMENT],
	doc DOCUMENT,
	docID DocID,
	add bool,
) {
	m := idx.fields[f].(map[FIELD]*PostingList)
	f.extractor(doc, func(field FIELD) {
		pl, ok := m[field]
		if add {
			if !ok {
				pl = NewPostingList()
				m[field] = pl
			}
			pl.Add(docID)
		} else if ok && pl.Remove(docID) {
			delete(m, field)
		}
	})
}

func (f *InvertedIndexField[DOCUMENT, FIELD]) Query(value FIELD) Query[DOCUMENT] {
	return queryFunc[DOCUMENT](func(index *InvertedIndex[DOCUMENT]) PostingListIterator {
		return index.fields[f].(map[FIELD]*PostingList)[value].Iterator()
	})
}

type FieldValueExtractor[DOCUMENT comparable] interface {
	newMap() any
	extractFieldValues(idx *InvertedIndex[DOCUMENT], doc DOCUMENT, docID DocID, add bool)
}

func NewInvertedIndexField[DOCUMENT, FIELD comparable](
	extractFieldValues func(doc DOCUMENT, f func(value FIELD)),
) *InvertedIndexField[DOCUMENT, FIELD] {
	return &InvertedIndexField[DOCUMENT, FIELD]{extractor: extractFieldValues}
}

func NewInvertedIndex[DOCUMENT comparable](fields ...FieldValueExtractor[DOCUMENT]) *InvertedIndex[DOCUMENT] {
	idx := &InvertedIndex[DOCUMENT]{
		fields:     make(map[FieldValueExtractor[DOCUMENT]]any, len(fields)),
		docIdToDoc: make(map[DocID]DOCUMENT),
		docToDocID: make(map[DOCUMENT]DocID),
	}
	for _, f := range fields {
		idx.fields[f] = f.newMap()
	}
	return idx
}

func (idx *InvertedIndex[DOCUMENT]) Add(doc DOCUMENT) bool {
	if _, ok := idx.docToDocID[doc]; ok {
		return false
	}

	idx.docCount++
	docID := idx.docCount
	idx.docIdToDoc[docID] = doc
	idx.docToDocID[doc] = docID

	for field := range idx.fields {
		field.extractFieldValues(idx, doc, docID, true)
	}

	return true
}

func (idx *InvertedIndex[DOCUMENT]) Remove(doc DOCUMENT) bool {
	docID, ok := idx.docToDocID[doc]
	if !ok {
		return false
	}

	delete(idx.docToDocID, doc)
	for field := range idx.fields {
		field.extractFieldValues(idx, doc, docID, false)
	}

	return true
}

func (idx *InvertedIndex[DOCUMENT]) Query(query Query[DOCUMENT], f func(DOCUMENT) bool) {
	Iterate(query.iterator(idx), func(docID DocID) bool {
		return f(idx.docIdToDoc[docID])
	})
}
