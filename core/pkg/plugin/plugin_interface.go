package plugin

import (
	"encoding/gob"
	"fmt"
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
		resp = []model.CustomCostResponse{
			{
				Errors: []error{
					fmt.Errorf("error calling plugin: %v", err),
				},
			},
		}
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

// this method is called for as part of the reference plugin implementation
// see https://github.com/hashicorp/go-plugin/blob/main/examples/basic/shared/greeter_interface.go#L59
// for context and details
func (p *CustomCostPlugin) Server(*plugin.MuxBroker) (interface{}, error) {
	return &CustomCostRPCServer{Impl: p.Impl}, nil
}

// this method is called for as part of the reference plugin implementation
// see https://github.com/hashicorp/go-plugin/blob/main/examples/basic/shared/greeter_interface.go#L63
// for context and details
func (CustomCostPlugin) Client(b *plugin.MuxBroker, c *rpc.Client) (interface{}, error) {
	return &CustomCostRPC{client: c}, nil
}
