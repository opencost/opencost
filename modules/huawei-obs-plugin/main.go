// Command huaweiobs is an OpenCost Custom Cost plugin (see
// core/pkg/plugin/plugin_interface.go and pkg/customcost/pipelineservice.go in the
// main module) that prices Huawei Cloud OBS bucket storage.
//
// OpenCost launches this as a subprocess: pkg/customcost/pipelineservice.go's
// getRegisteredPlugins runs
// "<PLUGIN_EXECUTABLE_DIR>/huaweiobs.ocplugin.<os>.<arch> <PLUGIN_CONFIG_DIR>/huaweiobs_config.json"
// -- see README.md for the full local setup.
package main

import (
	"os"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"

	ocplugin "github.com/opencost/opencost/core/pkg/plugin"
)

func main() {
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   "huaweiobs",
		Output: os.Stderr,
		Level:  hclog.Debug,
	})

	if len(os.Args) < 2 {
		logger.Error("missing required config file path argument")
		os.Exit(1)
	}

	cfg, err := loadConfig(os.Args[1])
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	handshakeConfig := plugin.HandshakeConfig{
		ProtocolVersion:  1,
		MagicCookieKey:   "PLUGIN_NAME",
		MagicCookieValue: "huaweiobs",
	}

	pluginMap := map[string]plugin.Plugin{
		"CustomCostSource": &ocplugin.CustomCostPlugin{Impl: NewObsCostSource(cfg)},
	}

	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
		GRPCServer:      plugin.DefaultGRPCServer,
		Logger:          logger,
	})
}
