package clusterstatus

import (
	"github.com/opencost/opencost/core/pkg/filter/fieldstrings"
)

type ClusterStatusField string

const (
	FieldClusterID      ClusterStatusField = ClusterStatusField(fieldstrings.FieldClusterID)
	FieldAccount        ClusterStatusField = ClusterStatusField(fieldstrings.FieldAccount)
	FieldCloudAccountID ClusterStatusField = "cloudAccountId"
	FieldProvider       ClusterStatusField = ClusterStatusField(fieldstrings.FieldProvider)
)

// Made with Bob
