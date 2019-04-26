module github.com/kubecost/cost-model

require github.com/kubecost/test/mocks v0.0.0

replace github.com/kubecost/test/mocks v0.0.0 => ./test/mocks

require (
	cloud.google.com/go v0.34.0
	github.com/aws/aws-sdk-go v1.19.10
	github.com/golang/mock v1.2.0
	github.com/googleapis/gax-go v2.0.2+incompatible // indirect
	github.com/jszwec/csvutil v1.2.1
	github.com/julienschmidt/httprouter v1.2.0
	github.com/prometheus/client_golang v0.9.3-0.20190127221311-3c4408c8b829
	golang.org/x/oauth2 v0.0.0-20190402181905-9f3314589c9a
	google.golang.org/api v0.3.0
	k8s.io/api v0.0.0-20190404065945-709cf190c7b7
	k8s.io/apimachinery v0.0.0-20190404065847-4a4abcd45006
	k8s.io/client-go v0.0.0-20190404172613-2e1a3ed22ac5
	k8s.io/klog v0.0.0-20190306015804-8e90cee79f82
)
