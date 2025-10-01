package plugin

import (
	"context"

	custompb "github.com/opencost/opencost/core/pkg/customcost/pb"
)

// GRPCClient is an implementation of CustomCostsSource that talks over RPC.
type GRPCClient struct{ client custompb.CustomCostsSourceClient }

func (m *GRPCClient) GetCustomCosts(req *custompb.CustomCostRequest) []*custompb.CustomCostResponse {
	resp, err := m.client.GetCustomCosts(context.Background(), req)
	if err != nil {
		return []*custompb.CustomCostResponse{
			{
				Errors: []string{err.Error()},
			},
		}
	}
	derefs := []*custompb.CustomCostResponse{}
	for _, resp := range resp.Resps {
		derefs = append(derefs, resp)
	}
	return derefs
}

// Here is the gRPC server that GRPCClient talks to.
type GRPCServer struct {
	custompb.UnimplementedCustomCostsSourceServer
	// This is the real implementation
	Impl CustomCostSource
}

func (m *GRPCServer) GetCustomCosts(
	ctx context.Context,
	req *custompb.CustomCostRequest) (*custompb.CustomCostResponseSet, error) {
	ptrs := []*custompb.CustomCostResponse{}
	costs := m.Impl.GetCustomCosts(req)
	for _, cost := range costs {
		ptrs = append(ptrs, cost)
	}
	return &custompb.CustomCostResponseSet{
		Resps: ptrs,
	}, nil
}
