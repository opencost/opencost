package oracle

import (
	"fmt"

	"github.com/opencost/opencost/core/pkg/util"
)

type oracleKey struct {
	gpuCount     int
	gpuType      string
	providerID   string
	instanceType string
	labels       map[string]string
}

func (k *oracleKey) ID() string {
	return k.providerID
}

// Features are the OCI node features: compute, memory, and optionally gpu.
func (k *oracleKey) Features() string {
	arch, ok := util.GetArchType(k.labels)
	if !ok {
		arch = "amd64"
	}
	return fmt.Sprintf("%s,%t,%s", k.instanceType, k.isVirtualNode(), arch)
}

func (k *oracleKey) GPUType() string {
	return k.gpuType
}

func (k *oracleKey) GPUCount() int {
	return k.gpuCount
}

func (k *oracleKey) isVirtualNode() bool {
	_, ok := k.labels[virtualNodeLabel]
	return ok
}

const driverOCI = "oracle.com/oci"
const driverOCIBV = "blockvolume.csi.oraclecloud.com"
const driverOCIFSS = "fss.csi.oraclecloud.com"

// ociStorageDrivers maps every known OCI storage driver to the part number
// of the product that prices its volumes.
var ociStorageDrivers = map[string]string{
	driverOCI:    blockVolumePartNumber,
	driverOCIBV:  blockVolumePartNumber,
	driverOCIFSS: fileStoragePartNumber,
}

type oraclePVKey struct {
	storageClass string
	driver       string
	providerID   string
	parameters   map[string]string
}

func (p *oraclePVKey) Features() string {
	return ociStorageDrivers[p.driver]
}

func (p *oraclePVKey) GetStorageClass() string {
	return p.storageClass
}

func (p *oraclePVKey) ID() string {
	return p.providerID
}
