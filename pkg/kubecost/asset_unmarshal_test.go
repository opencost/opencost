package kubecost

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func TestNode_Unmarshal(t *testing.T) {

	var s time.Time
	var e time.Time
	unmarshalWindow := NewWindow(&s, &e)

	node1 := NewNode("node1", "cluster1", "provider1", *unmarshalWindow.start, *unmarshalWindow.end, unmarshalWindow)

	node1.CPUCost = 1000

	fmt.Println(node1)
	bytes, _ := json.Marshal(node1)
	//fmt.Println(string(bytes))
	var node2 Node
	//fmt.Println(node2)
	err := json.Unmarshal(bytes, &node2)
	fmt.Println("error:", err)
	fmt.Println(node2.CPUCost)
	//bytes2, _ := json.Marshal(&node2)
	//fmt.Println(string(bytes2))
}
