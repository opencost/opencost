package kubecost

import (
	"bytes"
	"encoding/json"
	"time"
)

// MarshalJSON implements json.Marshaler interface
func (a *Allocation) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString("{")
	jsonEncodeString(buffer, "name", a.Name, ",")
	jsonEncode(buffer, "properties", a.Properties, ",")
	jsonEncode(buffer, "window", a.Window, ",")
	jsonEncodeString(buffer, "start", a.Start.Format(time.RFC3339), ",")
	jsonEncodeString(buffer, "end", a.End.Format(time.RFC3339), ",")
	jsonEncodeFloat64(buffer, "minutes", a.Minutes(), ",")
	jsonEncodeFloat64(buffer, "cpuCores", a.CPUCores(), ",")
	jsonEncodeFloat64(buffer, "cpuCoreRequestAverage", a.CPUCoreRequestAverage, ",")
	jsonEncodeFloat64(buffer, "cpuCoreUsageAverage", a.CPUCoreUsageAverage, ",")
	jsonEncodeFloat64(buffer, "cpuCoreHours", a.CPUCoreHours, ",")
	jsonEncodeFloat64(buffer, "cpuCost", a.CPUCost, ",")
	jsonEncodeFloat64(buffer, "cpuCostAdjustment", a.CPUCostAdjustment, ",")
	jsonEncodeFloat64(buffer, "cpuEfficiency", a.CPUEfficiency(), ",")
	jsonEncodeFloat64(buffer, "gpuCount", a.GPUs(), ",")
	jsonEncodeFloat64(buffer, "gpuHours", a.GPUHours, ",")
	jsonEncodeFloat64(buffer, "gpuCost", a.GPUCost, ",")
	jsonEncodeFloat64(buffer, "gpuCostAdjustment", a.GPUCostAdjustment, ",")
	jsonEncodeFloat64(buffer, "networkTransferBytes", a.NetworkTransferBytes, ",")
	jsonEncodeFloat64(buffer, "networkReceiveBytes", a.NetworkReceiveBytes, ",")
	jsonEncodeFloat64(buffer, "networkCost", a.NetworkCost, ",")
	jsonEncodeFloat64(buffer, "networkCostAdjustment", a.NetworkCostAdjustment, ",")
	jsonEncodeFloat64(buffer, "loadBalancerCost", a.LoadBalancerCost, ",")
	jsonEncodeFloat64(buffer, "loadBalancerCostAdjustment", a.LoadBalancerCostAdjustment, ",")
	jsonEncodeFloat64(buffer, "pvBytes", a.PVBytes(), ",")
	jsonEncodeFloat64(buffer, "pvByteHours", a.PVByteHours(), ",")
	jsonEncodeFloat64(buffer, "pvCost", a.PVCost(), ",")
	jsonEncode(buffer, "pvs", a.PVs, ",")
	jsonEncodeFloat64(buffer, "pvCostAdjustment", a.PVCostAdjustment, ",")
	jsonEncodeFloat64(buffer, "ramBytes", a.RAMBytes(), ",")
	jsonEncodeFloat64(buffer, "ramByteRequestAverage", a.RAMBytesRequestAverage, ",")
	jsonEncodeFloat64(buffer, "ramByteUsageAverage", a.RAMBytesUsageAverage, ",")
	jsonEncodeFloat64(buffer, "ramByteHours", a.RAMByteHours, ",")
	jsonEncodeFloat64(buffer, "ramCost", a.RAMCost, ",")
	jsonEncodeFloat64(buffer, "ramCostAdjustment", a.RAMCostAdjustment, ",")
	jsonEncodeFloat64(buffer, "ramEfficiency", a.RAMEfficiency(), ",")
	jsonEncodeFloat64(buffer, "sharedCost", a.SharedCost, ",")
	jsonEncodeFloat64(buffer, "externalCost", a.ExternalCost, ",")
	jsonEncodeFloat64(buffer, "totalCost", a.TotalCost(), ",")
	jsonEncodeFloat64(buffer, "totalEfficiency", a.TotalEfficiency(), ",")
	jsonEncode(buffer, "rawAllocationOnly", a.RawAllocationOnly, "")
	buffer.WriteString("}")
	return buffer.Bytes(), nil
}

// UnmarshalJSON prevent nil pointer on PVAllocations
func (a *Allocation) UnmarshalJSON(b []byte) error {
	// initialize PV to prevent nil panic
	a.PVs = PVAllocations{}
	// Aliasing Allocation and casting to alias gives access to the default unmarshaller
	type alloc Allocation
	err := json.Unmarshal(b, (*alloc)(a))
	if err != nil {
		return err
	}
	// clear PVs if they are empty, it is not initialized when empty
	if len(a.PVs) == 0 {
		a.PVs = nil
	}
	return nil
}

// MarshalJSON marshals PVAllocation as map[*PVKey]*PVAllocation this allows PVKey to retain its values through marshalling
func (pv PVAllocations) MarshalJSON() (b []byte, err error) {
	pointerMap := make(map[*PVKey]*PVAllocation)
	for pvKey, pvAlloc := range pv {
		kp := pvKey
		pointerMap[&kp] = pvAlloc
	}
	return json.Marshal(pointerMap)
}

// MarshalText converts PVKey to string to make it compatible with JSON Marshaller as an Object key
// this function is required to have a value caller for the actual values to be saved
func (pvk PVKey) MarshalText() (text []byte, err error) {
	return []byte(pvk.String()), nil
}

// UnmarshalText converts JSON key string to PVKey it compatible with JSON Unmarshaller from an Object key
// this function is required to have a pointer caller for values to be pulled into marshalling struct
func (pvk *PVKey) UnmarshalText(text []byte) error {
	return pvk.FromString(string(text))
}

// MarshalJSON JSON-encodes the AllocationSet
func (as *AllocationSet) MarshalJSON() ([]byte, error) {
	if as == nil {
		return json.Marshal(map[string]*Allocation{})
	}
	return json.Marshal(as.Allocations)
}

// MarshalJSON JSON-encodes the range
func (asr *AllocationSetRange) MarshalJSON() ([]byte, error) {
	if asr == nil {
		return json.Marshal([]*AllocationSet{})
	}

	return json.Marshal(asr.Allocations)
}
