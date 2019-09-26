module github.com/kubecost/cost-model

require github.com/kubecost/test/mocks v0.0.0

replace github.com/kubecost/test/mocks v0.0.0 => ./test/mocks

replace github.com/golang/lint => golang.org/x/lint v0.0.0-20180702182130-06c8688daad7

require (
	cloud.google.com/go v0.34.0
	contrib.go.opencensus.io/exporter/ocagent v0.5.0 // indirect
	github.com/Azure/azure-sdk-for-go v24.1.0+incompatible
	github.com/Azure/go-autorest v11.3.2+incompatible
	github.com/aws/aws-sdk-go v1.19.10
	github.com/dimchansky/utfbom v1.1.0 // indirect
	github.com/golang/groupcache v0.0.0-20190129154638-5b532d6fd5ef // indirect
	github.com/golang/mock v1.2.0
	github.com/google/martian v2.1.0+incompatible // indirect
	github.com/googleapis/gax-go v2.0.2+incompatible // indirect
	github.com/gophercloud/gophercloud v0.2.0 // indirect
	github.com/imdario/mergo v0.3.7 // indirect
	github.com/jszwec/csvutil v1.2.1
	github.com/julienschmidt/httprouter v1.2.0
	github.com/lib/pq v1.2.0
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/pkg/errors v0.8.1 // indirect
	github.com/prometheus/client_golang v1.0.0
	github.com/prometheus/client_model v0.0.0-20190129233127-fd36f4220a90
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/shopspring/decimal v0.0.0-20180709203117-cd690d0c9e24 // indirect
	golang.org/x/crypto v0.0.0-20190510104115-cbcb75029529 // indirect
	golang.org/x/lint v0.0.0-20190909230951-414d861bb4ac // indirect
	golang.org/x/oauth2 v0.0.0-20190604053449-0f29369cfe45
	golang.org/x/sync v0.0.0-20190423024810-112230192c58 // indirect
	google.golang.org/api v0.4.0
	gotest.tools v2.2.0+incompatible
	k8s.io/api v0.0.0-20190913080256-21721929cffa
	k8s.io/apimachinery v0.0.0-20190913075812-e119e5e154b6
	k8s.io/client-go v0.0.0-20190620085101-78d2af792bab
	k8s.io/klog v0.4.0
)
