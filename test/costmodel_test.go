package costmodel_test

import (
	"net/url"
	"testing"

	"github.com/golang/mock/gomock"
	costModel "github.com/kubecost/cost-model/costmodel"
	"github.com/kubecost/test/mocks"
)

func TestCostModel(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	u, _ := url.Parse("http://localhost:9003")
	cli := mocks.NewMockClient(ctrl)
	cli.EXPECT().URL(gomock.Any(), gomock.Any()).AnyTimes().Return(u)
	cli.EXPECT().Do(gomock.Any(), gomock.Any()).AnyTimes()

	clientset := mocks.NewMockInterface(ctrl)
	provider := mocks.NewMockProvider(ctrl)

	costModel.ComputeCostData(cli, clientset, provider, "1d")
}
