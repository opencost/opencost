package main

import (
	"github.com/opencost/opencost/pkg/cmd"
	"github.com/rs/zerolog/log"
)

func main() {
	// runs the appropriate application mode using the default cost-model command
	// see: github.com/opencost/opencost/pkg/cmd package for details
	// 
	// Note: startDM2Emitter hook is defined in dm2_hook_enabled.go (with dm2emitter build tag)
	// or dm2_hook_disabled.go (without build tag) and needs to be called from the actual
	// costmodel initialization code in pkg/cmd/costmodel/costmodel.go after caches are ready
	if err := cmd.Execute(nil); err != nil {
		log.Fatal().Err(err)
	}
}
