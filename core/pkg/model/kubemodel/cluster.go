package kubemodel

type Cluster struct {
	ID       string
	Provider Provider
	Account  string
	Name     string
	Window   Window
}
