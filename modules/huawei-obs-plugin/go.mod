module github.com/opencost/opencost/modules/huawei-obs-plugin

go 1.26.4

replace github.com/opencost/opencost/core => ./../../core

require (
	github.com/hashicorp/go-hclog v1.6.3
	github.com/hashicorp/go-plugin v1.7.0
	github.com/huaweicloud/huaweicloud-sdk-go-obs v3.26.6+incompatible
	github.com/huaweicloud/huaweicloud-sdk-go-v3 v0.1.208
	github.com/opencost/opencost/core v0.0.0-00010101000000-000000000000
	github.com/shopspring/decimal v1.4.0
	google.golang.org/protobuf v1.36.12-0.20260120151049-f2248ac996af
)

require (
	github.com/fatih/color v1.18.0 // indirect
	github.com/goccy/go-yaml v1.9.8 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/hashicorp/yamux v0.1.2 // indirect
	github.com/json-iterator/go v1.1.13-0.20220915233716-71ac16282d12 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.3-0.20250322232337-35a7c28c31ee // indirect
	github.com/oklog/run v1.2.0 // indirect
	github.com/tjfoc/gmsm v1.4.1 // indirect
	go.mongodb.org/mongo-driver v1.17.7 // indirect
	go.opentelemetry.io/otel v1.44.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.44.0 // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
	google.golang.org/grpc v1.82.1 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
)
