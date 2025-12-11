package kubemodel

import "time"

type Pod struct {
	UID                  string            `json:"uid"`
	NamespaceUID         string            `json:"namespaceUid"`
	OwnerUID             string            `json:"ownerUid"`
	NodeUID              string            `json:"nodeUid"`
	Name                 string            `json:"name"`
	Labels               map[string]string `json:"labels,omitempty"`
	Annotations          map[string]string `json:"annotations,omitempty"`
	Start                time.Time         `json:"start"`
	End                  time.Time         `json:"end"`
	CpuMillicoreUsageMax uint64            `json:"cpuMillicoreUsageMax"`
	RAMByteUsageMax      uint64            `json:"ramByteUsageMax"`
	NetworkTransferBytes uint64            `json:"networkTransferBytes"`
	NetworkReceiveBytes  uint64            `json:"networkReceiveBytes"`
}
