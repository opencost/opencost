package clusterstatus

import (
	"github.com/opencost/opencost/core/pkg/filter/fieldstrings"
)

type ClusterStatusField string

const (
	FieldClusterID      ClusterStatusField = ClusterStatusField(fieldstrings.FieldClusterID)
	FieldAccount        ClusterStatusField = ClusterStatusField(fieldstrings.FieldAccount)
	FieldCloudAccountID ClusterStatusField = ClusterStatusField(fieldstrings.FieldAccountID)
	FieldProvider       ClusterStatusField = ClusterStatusField(fieldstrings.FieldProvider)
)
