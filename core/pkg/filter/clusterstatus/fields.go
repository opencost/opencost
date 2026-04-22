package clusterstatus

import (
	"github.com/opencost/opencost/core/pkg/filter/fieldstrings"
)

type ClusterStatusField string

const (
	FieldCluster        ClusterStatusField = ClusterStatusField(fieldstrings.FieldClusterID)
	FieldAccountID      ClusterStatusField = ClusterStatusField(fieldstrings.FieldAccountID)
	FieldCloudAccountID ClusterStatusField = "cloudAccountId"
	FieldProvider       ClusterStatusField = ClusterStatusField(fieldstrings.FieldProvider)
)

// Made with Bob
