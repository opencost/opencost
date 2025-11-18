package kubemodel

import "time"

// @bingen:generate:Pod
type Pod struct {
	UID                        string            `json:"uid"`                        // @bingen:field[version=1]
	NamespaceUID               string            `json:"namespaceUid"`               // @bingen:field[version=1]
	ControllerUID              string            `json:"controllerUid"`              // @bingen:field[version=1]
	NodeUID                    string            `json:"nodeUid"`                    // @bingen:field[version=1]
	Name                       string            `json:"name"`                       // @bingen:field[version=1]
	Labels                     map[string]string `json:"labels,omitempty"`           // @bingen:field[version=1]
	Annotations                map[string]string `json:"annotations,omitempty"`      // @bingen:field[version=1]
	Start                      time.Time         `json:"start"`                      // @bingen:field[version=1]
	End                        time.Time         `json:"end"`                        // @bingen:field[version=1]
	CpuMillicoreSeconds        uint64            `json:"cpuMillicoreSeconds"`        // @bingen:field[version=1]
	CpuMillicoreRequestAverage uint64            `json:"cpuMillicoreRequestAverage"` // @bingen:field[version=1]
	CpuMillicoreUsageAverage   uint64            `json:"cpuMillicoreUsageAverage"`   // @bingen:field[version=1]
	CpuMillicoreUsageMax       uint64            `json:"cpuMillicoreUsageMax"`       // @bingen:field[version=1]
	RAMByteSeconds             uint64            `json:"ramByteSeconds"`             // @bingen:field[version=1]
	RAMByteRequestAverage      uint64            `json:"ramByteRequestAverage"`      // @bingen:field[version=1]
	RAMByteUsageAverage        uint64            `json:"ramByteUsageAverage"`        // @bingen:field[version=1]
	RAMByteUsageMax            uint64            `json:"ramByteUsageMax"`            // @bingen:field[version=1]
	StorageByteSeconds         uint64            `json:"storageByteSeconds"`         // @bingen:field[version=1]
	NetworkTransferBytes       uint64            `json:"networkTransferBytes"`       // @bingen:field[version=1]
	NetworkReceiveBytes        uint64            `json:"networkReceiveBytes"`        // @bingen:field[version=1]
	Diagnostic                 *DiagnosticResult `json:"diagnostic,omitempty"`       // @bingen:field[version=1]
}
