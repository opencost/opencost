package costmodel_test

// Mocks can be regenerated with something like GO111MODULE=on mockgen -destination ./test/mocks/mock_provider.go  -package mocks github.com/kubecost/cost-model/cloud Provider

import (
	"net/url"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/kubecost/test/mocks"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	fakecontroller "k8s.io/client-go/tools/cache/testing"
)

func TestCostModel(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	u, _ := url.Parse("http://localhost:9003")
	cli := mocks.NewMockClient(ctrl)
	cli.EXPECT().URL(gomock.Any(), gomock.Any()).AnyTimes().Return(u)
	cli.EXPECT().Do(gomock.Any(), gomock.Any()).AnyTimes()

	fc := fakecontroller.NewFakeControllerSource()
	fc.Add(&v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo",
		},
	})
	time.Sleep(100 * time.Millisecond)
}
