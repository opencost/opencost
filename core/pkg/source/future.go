package source

// ResultDecoder[T] is a function that decodes a `QueryResult` into a `*T` type.
type ResultDecoder[T any] func(*QueryResult) *T

// Future[T] is a async future/promise/task that will resolve into a []*T return or
// error when awaited. This type provides a type-safe way of interfacing with `QueryResultsChan`
// via a `ResultDecoder[T]` function.
type Future[T any] struct {
	decoder     ResultDecoder[T]
	resultsChan QueryResultsChan

	// results is set when we use a passthrough
	results []*T
}

// NewFuture Creates a new `Future[T]` with the given `ResultDecoder[T]` and `QueryResultsChan`.
func NewFuture[T any](decoder ResultDecoder[T], resultsChan QueryResultsChan) *Future[T] {
	return &Future[T]{
		decoder:     decoder,
		resultsChan: resultsChan,
	}
}

// NewFutureFrom accepts a result set to wrap in the a Future implementation for passthrough.
func NewFutureFrom[T any](results []*T) *Future[T] {
	return &Future[T]{
		results: results,
	}
}

// awaitWith allows internal callers to pass an error collector for grouping futures
func (f *Future[T]) awaitWith(errorCollector *QueryErrorCollector) ([]*T, error) {
	if f.results != nil {
		return f.results, nil
	}

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

// Await blocks and waits for the `Future` to resolve, and returns the results if successful, or an error
// otherwise.
func (f *Future[T]) Await() ([]*T, error) {
	// in the event that we have a resolved future, we can return the results directly
	if f.results != nil {
		return f.results, nil
	}

	results, err := f.resultsChan.Await()
	if err != nil {
		return nil, err
	}

	decoded := DecodeAll(results, f.decoder)
	return decoded, nil
}
