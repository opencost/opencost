package cloudcost

type RepositoryFactory interface {
	GetRepository(string) Repository
}
