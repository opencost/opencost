package pricing

type Provider string

const (
	NilProvider    Provider = ""
	AllProvider    Provider = "all"
	AWSProvider    Provider = "aws"
	AzureProvider  Provider = "azure"
	CustomProvider Provider = "custom"
	GCPProvider    Provider = "gcp"
)
