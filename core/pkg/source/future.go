package source

type ResultDecoder[T any] func(*QueryResult) *T

type Future[T any] struct {
	decoder     ResultDecoder[T]
	resultsChan QueryResultsChan
}

func NewFuture[T any](decoder ResultDecoder[T], resultsChan QueryResultsChan) *Future[T] {
	return &Future[T]{
		decoder:     decoder,
		resultsChan: resultsChan,
	}
}

// awaitWith allows internal callers to pass an error collector for grouping futures
func (f *Future[T]) awaitWith(errorCollector *QueryErrorCollector) ([]*T, error) {
	defer close(f.resultsChan)
	result := <-f.resultsChan

	q := result.Query
	err := result.Error

	if err != nil {
		errorCollector.AppendError(&QueryError{Query: q, Error: err})
		return nil, err
	}

	decoded := DecodeAll(result.Results, f.decoder)
	return decoded, nil
}

func (f *Future[T]) Await() ([]*T, error) {
	results, err := f.resultsChan.Await()
	if err != nil {
		return nil, err
	}

	decoded := DecodeAll(results, f.decoder)
	return decoded, nil
}
