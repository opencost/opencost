package source

type QueryGroup struct {
	errorCollector *QueryErrorCollector
}

type QueryGroupAsyncResult struct {
	errorCollector *QueryErrorCollector
	resultsChan    QueryResultsChan
}

type QueryGroupFuture[T any] struct {
	errorCollector *QueryErrorCollector
	future         *Future[T]
}

func WithGroup[T any](g *QueryGroup, f *Future[T]) *QueryGroupFuture[T] {
	return &QueryGroupFuture[T]{
		errorCollector: g.errorCollector,
		future:         f,
	}
}

func (qgf *QueryGroupFuture[T]) Await() ([]*T, error) {
	return qgf.future.awaitWith(qgf.errorCollector)
}

func NewQueryGroup() *QueryGroup {
	var errorCollector QueryErrorCollector

	return &QueryGroup{
		errorCollector: &errorCollector,
	}
}

func (qg *QueryGroup) With(resultsChan QueryResultsChan) *QueryGroupAsyncResult {
	return newQueryGroupAsyncResult(qg.errorCollector, resultsChan)
}

func (qg *QueryGroup) HasErrors() bool {
	return qg.errorCollector.IsError()
}

func (qg *QueryGroup) Error() error {
	return qg.errorCollector
}

func (qg *QueryGroup) Errors() []*QueryError {
	return qg.errorCollector.Errors()
}

func newQueryGroupAsyncResult(collector *QueryErrorCollector, resultsChan QueryResultsChan) *QueryGroupAsyncResult {
	return &QueryGroupAsyncResult{
		errorCollector: collector,
		resultsChan:    resultsChan,
	}
}

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
