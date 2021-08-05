module github.com/kubecost/cost-model

replace github.com/golang/lint => golang.org/x/lint v0.0.0-20180702182130-06c8688daad7

require (
	cloud.google.com/go v0.54.0
	cloud.google.com/go/bigquery v1.4.0
	github.com/Azure/azure-sdk-for-go v51.1.0+incompatible
	github.com/Azure/azure-storage-blob-go v0.13.0
	github.com/Azure/go-autorest/autorest v0.11.17
	github.com/Azure/go-autorest/autorest/azure/auth v0.5.6
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.1 // indirect
	github.com/aws/aws-sdk-go v1.28.9
	github.com/davecgh/go-spew v1.1.1
	github.com/getsentry/sentry-go v0.6.1
	github.com/google/uuid v1.1.2
	github.com/json-iterator/go v1.1.11
	github.com/jszwec/csvutil v1.2.1
	github.com/julienschmidt/httprouter v1.3.0
	github.com/lib/pq v1.2.0
	github.com/microcosm-cc/bluemonday v1.0.2
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/prometheus/client_golang v1.0.0
	github.com/prometheus/client_model v0.2.0
	github.com/rs/cors v1.7.0
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/shopspring/decimal v0.0.0-20180709203117-cd690d0c9e24 // indirect
	go.etcd.io/bbolt v1.3.5
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	google.golang.org/api v0.20.0
	gopkg.in/yaml.v2 v2.3.0
	k8s.io/api v0.20.4
	k8s.io/apimachinery v0.20.4
	k8s.io/client-go v0.20.4
	k8s.io/klog v0.4.0
	sigs.k8s.io/yaml v1.2.0
)

go 1.13
