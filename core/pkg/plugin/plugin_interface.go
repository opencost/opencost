package plugin

import (
	"encoding/gob"
	"net/rpc"

	"github.com/hashicorp/go-plugin"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model"
)

// plugin interface
type CustomCostSource interface {
	GetCustomCosts(req model.CustomCostRequestInterface) []model.CustomCostResponse
}

// RPC impl
type CustomCostRPC struct{ client *rpc.Client }

func init() {
	gob.Register(model.CustomCostRequest{})
}
func (c *CustomCostRPC) GetCustomCosts(req model.CustomCostRequestInterface) []model.CustomCostResponse {

	var resp []model.CustomCostResponse
	err := c.client.Call("Plugin.GetCustomCosts", &req, &resp)
	if err != nil {
		log.Errorf("error calling plugin: %v", err)
	}

	return resp
}

type CustomCostRPCServer struct {
	// This is the real implementation
	Impl CustomCostSource
}

func (s *CustomCostRPCServer) GetCustomCosts(args interface{}, resp *[]model.CustomCostResponse) error {
	*resp = s.Impl.GetCustomCosts(args.(model.CustomCostRequestInterface))
	return nil
}

type CustomCostPlugin struct {
	// Impl Injection
	Impl CustomCostSource
}

func (p *CustomCostPlugin) Server(*plugin.MuxBroker) (interface{}, error) {
	return &CustomCostRPCServer{Impl: p.Impl}, nil
}

func (CustomCostPlugin) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &CustomCostRPC{client: c}, nil
}
