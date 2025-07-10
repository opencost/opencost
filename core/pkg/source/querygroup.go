package source

// QueryGroupAsyncResult is a representation of a single async query in a group.
type QueryGroupAsyncResult struct {
	errorCollector *QueryErrorCollector
	resultsChan    QueryResultsChan
}

// newQueryGroupAsyncResult creates a new QueryGroupAsyncResult with the given error collector and results channel.
func newQueryGroupAsyncResult(collector *QueryErrorCollector, resultsChan QueryResultsChan) *QueryGroupAsyncResult {
	return &QueryGroupAsyncResult{
		errorCollector: collector,
		resultsChan:    resultsChan,
	}
}

// Await blocks and waits for the `QueryGroupAsyncResult` to resolve, and returns a slice of generic `QueryResult`
// instances if successful, or an error otherwise.
func (qgar *QueryGroupAsyncResult) Await() ([]*QueryResult, error) {
	defer close(qgar.resultsChan)
	result := <-qgar.resultsChan

	q := result.Query
	err := result.Error

	if err != nil {
		qgar.errorCollector.AppendError(&QueryError{Query: q, Error: err})
		return nil, err
	}

	return result.Results, nil
}

// QueryGroupFuture[T] is a representation of a single async query in a group with a typed result.
type QueryGroupFuture[T any] struct {
	errorCollector *QueryErrorCollector
	future         *Future[T]
}

// WithGroup creates a new QueryGroupFuture[T] instance with the given QueryGroup and Future instances.
// This is the specific way to add a typed `Future[T]` to a `QueryGroup`.
func WithGroup[T any](g *QueryGroup, f *Future[T]) *QueryGroupFuture[T] {
	return &QueryGroupFuture[T]{
		errorCollector: g.errorCollector,
		future:         f,
	}
}

// Await blocks and waits for the `QueryGroupFuture[T]` to resolve, and returns a slice of `*T` instances if successful,
// or an error otherwise.
func (qgf *QueryGroupFuture[T]) Await() ([]*T, error) {
	return qgf.future.awaitWith(qgf.errorCollector)
}

// QueryGroup is a representation of multiple async queries. It provides a shared error collector
// for all queries in the group.
//
// Example:
//
//	grp := NewQueryGroup()
//	q1 := WithGroup(grp, QueryFoo())
//	q2 := WithGroup(grp, QueryBar())
//
//	results1, _ := q1.Await()
//	results2, _ := q2.Await()
//
//	if grp.HasErrors() {
//		return grp.Error() // <-- error return type
//	}
type QueryGroup struct {
	errorCollector *QueryErrorCollector
}

// NewQueryGroup creates a new QueryGroup instance which can be used to group non-typed async queries with
// the `With(QueryResultsChan)` instance method, or with the package function `WithGroup[T](*QueryGroup, *Future[T])`
func NewQueryGroup() *QueryGroup {
	var errorCollector QueryErrorCollector

	return &QueryGroup{
		errorCollector: &errorCollector,
	}
}

// With adds the given `QueryResultsChan` to the QueryGroup instance and returns a `QueryGroupAsyncResult` instance to be
// awaited
func (qg *QueryGroup) With(resultsChan QueryResultsChan) *QueryGroupAsyncResult {
	return newQueryGroupAsyncResult(qg.errorCollector, resultsChan)
}

// HasErrors returns true if any of the async queries in the group have errored. Note that all results must be awaited
// in order to be checked for errors.
func (qg *QueryGroup) HasErrors() bool {
	return qg.errorCollector.IsError()
}

// Error returns nil if there were no errors in the group. Otherwise, it returns all of the errors that occurred grouped
// into a single error.
func (qg *QueryGroup) Error() error {
	if !qg.errorCollector.IsError() {
		var err error
		return err
	}

	return qg.errorCollector
}

// Errors returns all of individual errors that occurred in the group.
func (qg *QueryGroup) Errors() []*QueryError {
	return qg.errorCollector.Errors()
}
