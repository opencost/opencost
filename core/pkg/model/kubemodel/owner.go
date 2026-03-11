package kubemodel

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
)

type OwnerKind string

const (
	OwnerKindDeployment  OwnerKind = "deployment"
	OwnerKindStatefulSet OwnerKind = "statefulset"
	OwnerKindDaemonSet   OwnerKind = "daemonset"
	OwnerKindJob         OwnerKind = "job"
	OwnerKindCronJob     OwnerKind = "cronjob"
	OwnerKindReplicaSet  OwnerKind = "replicaset"
)

func ParseOwnerKind(kind string) OwnerKind {
	switch strings.ToLower(kind) {
	case "deployment":
		return OwnerKindDeployment
	case "statefulset":
		return OwnerKindStatefulSet
	case "daemonset":
		return OwnerKindDaemonSet
	case "job":
		return OwnerKindJob
	case "cronjob":
		return OwnerKindCronJob
	case "replicaset":
		return OwnerKindReplicaSet
	default:
		log.Warnf("failed to find owner kind for '%s'", kind)
		return OwnerKind(strings.ToLower(kind))
	}
}

type Owner struct {
	UID  string
	Kind OwnerKind
}

func (o Owner) MarshalBinary() ([]byte, error) {
	uid := []byte(o.UID)
	kind := []byte(o.Kind)
	buf := make([]byte, 8+len(uid)+len(kind))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(uid)))
	copy(buf[4:], uid)
	offset := 4 + len(uid)
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(kind)))
	copy(buf[offset+4:], kind)
	return buf, nil
}

func (o *Owner) UnmarshalBinary(data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("Owner.UnmarshalBinary: insufficient data")
	}
	uidLen := int(binary.LittleEndian.Uint32(data[0:4]))
	if len(data) < 4+uidLen+4 {
		return fmt.Errorf("Owner.UnmarshalBinary: insufficient data for UID")
	}
	o.UID = string(data[4 : 4+uidLen])
	offset := 4 + uidLen
	kindLen := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	if len(data) < offset+4+kindLen {
		return fmt.Errorf("Owner.UnmarshalBinary: insufficient data for Kind")
	}
	o.Kind = OwnerKind(data[offset+4 : offset+4+kindLen])
	return nil
}
