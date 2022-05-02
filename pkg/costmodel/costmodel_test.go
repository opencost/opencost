package costmodel

import (
	"testing"
)

func Test_CostData_GetController_CronJob(t *testing.T) {
	cases := []struct {
		name string
		cd   CostData

		expectedName          string
		expectedKind          string
		expectedHasController bool
	}{
		{
			name: "batch/v1beta1 CronJob Job name",
			cd: CostData{
				// batch/v1beta1 CronJobs create Jobs with a 10 character
				// timestamp appended to the end of the name.
				//
				// It looks like this:
				// CronJob: cronjob-1
				// Job: cronjob-1-1651057200
				// Pod: cronjob-1-1651057200-mf5c9
				Jobs: []string{"cronjob-1-1651057200"},
			},

			expectedName:          "cronjob-1",
			expectedKind:          "job",
			expectedHasController: true,
		},
		{
			name: "batch/v1 CronJob Job name",
			cd: CostData{
				// batch/v1CronJobs create Jobs with an 8 character timestamp
				// appended to the end of the name.
				//
				// It looks like this:
				// CronJob: cj-v1
				// Job: cj-v1-27517770
				// Pod: cj-v1-27517770-xkrgn
				Jobs: []string{"cj-v1-27517770"},
			},

			expectedName:          "cj-v1",
			expectedKind:          "job",
			expectedHasController: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			name, kind, hasController := c.cd.GetController()

			if name != c.expectedName {
				t.Errorf("Name mismatch. Expected: %s. Got: %s", c.expectedName, name)
			}
			if kind != c.expectedKind {
				t.Errorf("Kind mismatch. Expected: %s. Got: %s", c.expectedKind, kind)
			}
			if hasController != c.expectedHasController {
				t.Errorf("HasController mismatch. Expected: %t. Got: %t", c.expectedHasController, hasController)
			}
		})
	}
}
