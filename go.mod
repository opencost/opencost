module github.com/kubecost/cost-model

require github.com/kubecost/test/mocks v0.0.0

replace github.com/kubecost/test/mocks v0.0.0 => ./test/mocks

require (
	cloud.google.com/go v0.34.0
	contrib.go.opencensus.io/exporter/ocagent v0.5.0 // indirect
	github.com/Azure/azure-sdk-for-go v24.1.0+incompatible
	github.com/Azure/go-autorest v11.3.2+incompatible
	github.com/aws/aws-sdk-go v1.19.10
	github.com/dimchansky/utfbom v1.1.0 // indirect
	github.com/golang/mock v1.2.0
	github.com/google/martian v2.1.0+incompatible // indirect
	github.com/googleapis/gax-go v2.0.2+incompatible // indirect
	github.com/jszwec/csvutil v1.2.1
	github.com/julienschmidt/httprouter v1.2.0
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/prometheus/client_golang v0.9.3-0.20190127221311-3c4408c8b829
	github.com/satori/go.uuid v1.2.0 // indirect
	github.com/shopspring/decimal v0.0.0-20180709203117-cd690d0c9e24 // indirect
	github.com/stretchr/testify v1.3.0 // indirect
	golang.org/x/crypto v0.0.0-20190418165655-df01cb2cc480 // indirect
	golang.org/x/oauth2 v0.0.0-20190402181905-9f3314589c9a
	google.golang.org/api v0.4.0
	gotest.tools v2.2.0+incompatible
	k8s.io/api v0.0.0-20190404065945-709cf190c7b7
	k8s.io/apimachinery v0.0.0-20190404065847-4a4abcd45006
	k8s.io/client-go v0.0.0-20190404172613-2e1a3ed22ac5
	k8s.io/klog v0.0.0-20190306015804-8e90cee79f82
)
