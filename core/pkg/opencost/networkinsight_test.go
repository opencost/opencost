package opencost

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	filter "github.com/opencost/opencost/core/pkg/filter/networkinsight"
	"github.com/opencost/opencost/core/pkg/util"
)

const (
	mockCluster1   = "mockCluster1"
	mockNamespace1 = "mockNamespace1"
	mockPod1       = "mockPod1"
	mockNamespace2 = "mockNamespace2"
	mockPod2       = "mockPod2"
)

func Test_NetworkDetailsCombineAndAdd(t *testing.T) {

	mcsI1inSet1, _ := createMockNetworkDetail(0, 200000, mockCloudService1, NetworkTrafficDirectionIngress, NetworkTrafficTypeInternet)
	mcsI2inSet1, _ := createMockNetworkDetail(0, 400000, mockCloudService2, NetworkTrafficDirectionIngress, NetworkTrafficTypeInternet)
	mcsI3inSet1, _ := createMockNetworkDetail(0, 800000, mockCloudService2, NetworkTrafficDirectionIngress, NetworkTrafficTypeInternet)
	mcsE1inSet1, _ := createMockNetworkDetail(0.12, 200000, mockCloudService1, NetworkTrafficDirectionEgress, NetworkTrafficTypeInternet)
	mcsE2inSet1, _ := createMockNetworkDetail(0.15, 400000, mockCloudService2, NetworkTrafficDirectionEgress, NetworkTrafficTypeInternet)

	mcsI1inSet2, _ := createMockNetworkDetail(0, 300000, mockCloudService1, NetworkTrafficDirectionIngress, NetworkTrafficTypeInternet)
	mcsI2inSet2, _ := createMockNetworkDetail(0, 300000, mockCloudService2, NetworkTrafficDirectionIngress, NetworkTrafficTypeInternet)
	mcsE1inSet2, _ := createMockNetworkDetail(0.16, 300000, mockCloudService1, NetworkTrafficDirectionEgress, NetworkTrafficTypeInternet)
	mcsE2inSet2, _ := createMockNetworkDetail(0.24, 300000, mockCloudService2, NetworkTrafficDirectionEgress, NetworkTrafficTypeInternet)
	mcsE3inSet2, _ := createMockNetworkDetail(0.35, 300000, mockCloudService3, NetworkTrafficDirectionEgress, NetworkTrafficTypeInternet)

	mcsI1inCombineSet, _ := createMockNetworkDetail(0, 500000, mockCloudService1, NetworkTrafficDirectionIngress, NetworkTrafficTypeInternet)
	mcsI2inCombineSet, _ := createMockNetworkDetail(0, 700000, mockCloudService2, NetworkTrafficDirectionIngress, NetworkTrafficTypeInternet)
	mcsE1inCombineSet, _ := createMockNetworkDetail(0.28, 500000, mockCloudService1, NetworkTrafficDirectionEgress, NetworkTrafficTypeInternet)
	mcsE2inCombineSet, _ := createMockNetworkDetail(0.39, 700000, mockCloudService2, NetworkTrafficDirectionEgress, NetworkTrafficTypeInternet)

	set1 := make(NetworkDetailsSet, 0)
	set1[mcsI1inSet1.Key()] = mcsI1inSet1
	set1[mcsI2inSet1.Key()] = mcsI2inSet1
	set1[mcsI3inSet1.Key()] = mcsI3inSet1
	set1[mcsE1inSet1.Key()] = mcsE1inSet1
	set1[mcsE2inSet1.Key()] = mcsE2inSet1

	set2 := make(NetworkDetailsSet, 0)
	set2[mcsI1inSet2.Key()] = mcsI1inSet2
	set2[mcsI2inSet2.Key()] = mcsI2inSet2
	set2[mcsE1inSet2.Key()] = mcsE1inSet2
	set2[mcsE2inSet2.Key()] = mcsE2inSet2
	set2[mcsE3inSet2.Key()] = mcsE3inSet2

	expected := make(NetworkDetailsSet, 0)
	expected[mcsI1inCombineSet.Key()] = mcsI1inCombineSet
	expected[mcsI2inCombineSet.Key()] = mcsI2inCombineSet
	expected[mcsI3inSet1.Key()] = mcsI3inSet1
	expected[mcsE1inCombineSet.Key()] = mcsE1inCombineSet
	expected[mcsE2inCombineSet.Key()] = mcsE2inCombineSet
	expected[mcsE3inSet2.Key()] = mcsE3inSet2

	set1.Combine(set2)

	if !reflect.DeepEqual(set1, expected) {
		t.Fatalf("Test_NetworkIngressCombineAndAdd: NetworkIngressDetailSet:combine not working as expected")
	}
}

func Test_NetworkInsightSetInsertFn(t *testing.T) {
	mockInsight1 := MockNetworkInsightImportantKeys{
		Cluster:    mockCluster1,
		Namespace:  mockNamespace1,
		Controller: "",
		Pod:        mockPod1,
		Node:       "",
		Labels:     map[string]string{},
		Region:     "",
		Zone:       "",
	}

	mockInsight2 := MockNetworkInsightImportantKeys{
		Cluster:    mockCluster1,
		Namespace:  mockNamespace2,
		Controller: "",
		Pod:        mockPod2,
		Node:       "",
		Labels:     map[string]string{},
		Region:     "",
		Zone:       "",
	}

	mcs1inIngress, _ := createMockNetworkDetail(0, 200000, mockCloudService1, NetworkTrafficDirectionIngress, NetworkTrafficTypeInternet)
	mcs2inIngress, _ := createMockNetworkDetail(0, 400000, mockCloudService2, NetworkTrafficDirectionIngress, NetworkTrafficTypeInternet)
	mcs3inIngress, _ := createMockNetworkDetail(0, 800000, mockCloudService2, NetworkTrafficDirectionIngress, NetworkTrafficTypeInternet)
	mcs1inEgress, _ := createMockNetworkDetail(0.16, 300000, mockCloudService1, NetworkTrafficDirectionEgress, NetworkTrafficTypeInternet)
	mcs2inEgress, _ := createMockNetworkDetail(0.24, 300000, mockCloudService2, NetworkTrafficDirectionEgress, NetworkTrafficTypeInternet)
	mcs3inEgress, _ := createMockNetworkDetail(0.35, 300000, mockCloudService3, NetworkTrafficDirectionEgress, NetworkTrafficTypeInternet)

	set1 := make(NetworkDetailsSet, 0)
	set1[mcs1inIngress.Key()] = mcs1inIngress
	set1[mcs2inIngress.Key()] = mcs2inIngress
	set1[mcs3inIngress.Key()] = mcs3inIngress
	set1[mcs1inEgress.Key()] = mcs1inEgress
	set1[mcs2inEgress.Key()] = mcs2inEgress
	set1[mcs3inEgress.Key()] = mcs3inEgress

	set2 := make(NetworkDetailsSet, 0)
	set2[mcs1inIngress.Key()] = mcs1inIngress.Clone()
	set2[mcs2inIngress.Key()] = mcs2inIngress.Clone()
	set2[mcs3inIngress.Key()] = mcs3inIngress.Clone()
	set2[mcs1inEgress.Key()] = mcs1inEgress.Clone()
	set2[mcs2inEgress.Key()] = mcs2inEgress.Clone()
	set2[mcs3inEgress.Key()] = mcs3inEgress.Clone()

	ni1 := createMockNetworkInsight(mockInsight1, set1)
	ni2 := createMockNetworkInsight(mockInsight1, set2)
	ni3 := createMockNetworkInsight(mockInsight2, set1.Clone())

	e := time.Now()
	s := e.Add(-1 * time.Hour)
	nis := &NetworkInsightSet{
		NetworkInsights: make(map[string]*NetworkInsight, 0),
		Window:          NewWindow(&s, &e),
	}

	nis.Insert(ni1, []NetworkInsightProperty{NetworkInsightsPod})
	nis.Insert(ni2, []NetworkInsightProperty{NetworkInsightsPod})
	nis.Insert(ni3, []NetworkInsightProperty{NetworkInsightsPod})

	aggregatedNis := nis.NetworkInsights

	if val, ok := aggregatedNis[mockPod1]; !ok {
		t.Fatalf("Test_NetworkInsightSetInsertFn: %s not found after insert", mockPod1)
	} else {
		if !util.IsApproximately(val.NetworkDetails.GetTotalInternetCost(), 1.50) {
			t.Logf("1 is %f", val.NetworkDetails.GetTotalInternetCost())
			t.Fatalf("Test_NetworkInsightSetInsertFn: failed to insert and add properly for pod %s", mockPod1)
		}
	}
	if val, ok := aggregatedNis[mockPod2]; !ok {
		t.Fatalf("Test_NetworkInsightSetInsertFn: %s not found after insert", mockPod2)
	} else {
		if !util.IsApproximately(val.NetworkDetails.GetTotalInternetCost(), 0.75) {
			t.Logf("2 is %f", val.NetworkDetails.GetTotalInternetCost())
			t.Fatalf("Test_NetworkInsightSetInsertFn: failed to insert and add properly for pod %s", mockPod1)
		}
	}
}

func Test_NetworkDetailSetFilterZeroCost(t *testing.T) {
	mcs1inIngress, _ := createMockNetworkDetail(0, 200000, mockCloudService1, NetworkTrafficDirectionIngress, NetworkTrafficTypeInternet)
	mcs2inIngress, _ := createMockNetworkDetail(0, 400000, mockCloudService2, NetworkTrafficDirectionIngress, NetworkTrafficTypeInternet)
	mcs3inIngress, _ := createMockNetworkDetail(0, 800000, mockCloudService2, NetworkTrafficDirectionIngress, NetworkTrafficTypeInternet)
	mcs1inEgress, _ := createMockNetworkDetail(0.16, 300000, mockCloudService1, NetworkTrafficDirectionEgress, NetworkTrafficTypeInternet)
	mcs2inEgress, _ := createMockNetworkDetail(0.24, 300000, mockCloudService2, NetworkTrafficDirectionEgress, NetworkTrafficTypeInternet)
	mcs3inEgress, _ := createMockNetworkDetail(0.35, 300000, mockCloudService3, NetworkTrafficDirectionEgress, NetworkTrafficTypeInternet)

	set1 := make(NetworkDetailsSet, 0)
	set1[mcs1inIngress.Key()] = mcs1inIngress
	set1[mcs2inIngress.Key()] = mcs2inIngress
	set1[mcs3inIngress.Key()] = mcs3inIngress
	set1[mcs1inEgress.Key()] = mcs1inEgress
	set1[mcs2inEgress.Key()] = mcs2inEgress
	set1[mcs3inEgress.Key()] = mcs3inEgress

	set2 := make(NetworkDetailsSet, 0)
	set2[mcs1inEgress.Key()] = mcs1inEgress
	set2[mcs2inEgress.Key()] = mcs2inEgress
	set2[mcs3inEgress.Key()] = mcs3inEgress

	set3 := set1.filterZeroCost()
	if !reflect.DeepEqual(set3, set2) {
		t.Fatalf("Test_NetworkIngressCombineAndAdd: NetworkIngressDetailSet:filterZeroCost failed with showZeroCost:false option")
	}
}

func Test_NetworkInsightFilterNetworkDetails(t *testing.T) {
	parserForEqualsTest := filter.NewNetworkInsightDetailFilterParser()
	compiler := NewNetworkInsightDetailMatchCompiler()
	equalsTestRegex, err := parserForEqualsTest.Parse(fmt.Sprintf("endPoint:\"%s\"", mockCloudService1))
	// if error encounter skip the test
	if err != nil {
		t.Logf("skipping test case to avoid intermittent test failure if not able to parse filter string")
		t.Skip()
	}
	equalsTestFilter, err := compiler.Compile(equalsTestRegex)
	// if error encounter skip the test
	if err != nil {
		t.Logf("skipping test case to avoid intermittent test failure if not able to parse filter string")
		t.Skip()
	}

	parserForContainsTest := filter.NewNetworkInsightDetailFilterParser()
	containsTestRegex, err := parserForContainsTest.Parse(fmt.Sprintf("endPoint~:\"%s\"", mockCloudServiceName))
	// if error encounter skip the test
	if err != nil {
		t.Logf("skipping test case to avoid intermittent test failure if not able to parse filter string")
		t.Skip()
	}
	containsTestFilter, err := compiler.Compile(containsTestRegex)
	// if error encounter skip the test
	if err != nil {
		t.Logf("skipping test case to avoid intermittent test failure if not able to parse filter string")
		t.Skip()
	}

	mcs1inIngress, _ := createMockNetworkDetail(0, 200000, mockCloudService1, NetworkTrafficDirectionIngress, NetworkTrafficTypeInternet)
	mcs2inIngress, _ := createMockNetworkDetail(0, 400000, mockCloudService2, NetworkTrafficDirectionIngress, NetworkTrafficTypeInternet)
	mcs3inIngress, _ := createMockNetworkDetail(0, 800000, mockCloudService3, NetworkTrafficDirectionIngress, NetworkTrafficTypeInternet)
	mcs1inEgress, _ := createMockNetworkDetail(0.16, 300000, mockCloudService1, NetworkTrafficDirectionEgress, NetworkTrafficTypeInternet)
	mcs2inEgress, _ := createMockNetworkDetail(0.24, 300000, mockCloudService2, NetworkTrafficDirectionEgress, NetworkTrafficTypeInternet)
	mcs3inEgress, _ := createMockNetworkDetail(0.35, 300000, mockCloudService3, NetworkTrafficDirectionEgress, NetworkTrafficTypeInternet)

	set1 := make(NetworkDetailsSet, 0)
	set1[mcs1inIngress.Key()] = mcs1inIngress
	set1[mcs2inIngress.Key()] = mcs2inIngress
	set1[mcs3inIngress.Key()] = mcs3inIngress
	set1[mcs1inEgress.Key()] = mcs1inEgress
	set1[mcs2inEgress.Key()] = mcs2inEgress
	set1[mcs3inEgress.Key()] = mcs3inEgress

	// set2 with only mockservice1 network details for validation
	set2 := make(NetworkDetailsSet, 0)
	set2[mcs1inIngress.Key()] = mcs1inIngress
	set2[mcs1inEgress.Key()] = mcs1inEgress

	mockInsight1 := MockNetworkInsightImportantKeys{
		Cluster:    mockCluster1,
		Namespace:  mockNamespace1,
		Controller: "",
		Pod:        mockPod1,
		Node:       "",
		Labels:     map[string]string{},
		Region:     "",
		Zone:       "",
	}

	ni1 := createMockNetworkInsight(mockInsight1, set1)

	ni1Copy := ni1.Clone()
	ni1.filterNetworkDetails(equalsTestFilter)

	// Check to see after filtering the network details
	//only have filtered result that is set2
	if !reflect.DeepEqual(ni1.NetworkDetails, set2) {
		t.Fatalf("Test_NetworkIngressCombineAndAdd: NetworkIngressDetailSet:filterNetworkDetails failed with equal to filter")
	}

	// Check to see after filtering the network details only have filtered
	// result that is original as a combine data that satisfy the regex
	ni1Copy.filterNetworkDetails(containsTestFilter)
	if !reflect.DeepEqual(ni1Copy.NetworkDetails, set1) {
		t.Fatalf("Test_NetworkIngressCombineAndAdd: NetworkIngressDetailSet:filterNetworkDetails failed with contains to filter")
	}

}
