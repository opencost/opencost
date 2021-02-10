module github.com/kubecost/cost-model

replace github.com/golang/lint => golang.org/x/lint v0.0.0-20180702182130-06c8688daad7

require (
	cloud.google.com/go v0.34.0
	github.com/Azure/azure-sdk-for-go v51.1.0+incompatible
	github.com/Azure/azure-storage-blob-go v0.13.0
	github.com/Azure/go-autorest/autorest v0.11.17
	github.com/Azure/go-autorest/autorest/azure/auth v0.5.6
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.1 // indirect
	github.com/aws/aws-sdk-go v1.28.9
	github.com/davecgh/go-spew v1.1.1
	github.com/getsentry/sentry-go v0.6.1
	github.com/google/martian v2.1.0+incompatible // indirect
	github.com/google/uuid v1.1.1
	github.com/googleapis/gax-go v2.0.2+incompatible // indirect
	github.com/gophercloud/gophercloud v0.2.0 // indirect
	github.com/jszwec/csvutil v1.2.1
	github.com/julienschmidt/httprouter v1.2.0
	github.com/lib/pq v1.2.0
	github.com/microcosm-cc/bluemonday v1.0.2
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/prometheus/client_golang v1.0.0
	github.com/prometheus/client_model v0.0.0-20190129233127-fd36f4220a90
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/shopspring/decimal v0.0.0-20180709203117-cd690d0c9e24 // indirect
	go.etcd.io/bbolt v1.3.5
	golang.org/x/oauth2 v0.0.0-20190604053449-0f29369cfe45
	golang.org/x/sync v0.0.0-20190423024810-112230192c58
	google.golang.org/api v0.4.0
	google.golang.org/grpc v1.20.1 // indirect
	gopkg.in/yaml.v2 v2.2.4
	k8s.io/api v0.0.0-20190913080256-21721929cffa
	k8s.io/apimachinery v0.0.0-20190913075812-e119e5e154b6
	k8s.io/client-go v0.0.0-20190620085101-78d2af792bab
	k8s.io/klog v0.4.0
	sigs.k8s.io/yaml v1.1.0
)

go 1.13
