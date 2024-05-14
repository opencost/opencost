package config

import (
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
	proto "github.com/opencost/opencost/core/pkg/protocol"
)

var protocol = proto.HTTP()

func (c *Controller) cloudCostChecks() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// If Pipeline is nil, always return 503
	if c == nil {
		return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
			http.Error(w, "ConfigController: is nil", http.StatusServiceUnavailable)
		}
	}

	return nil
}

// GetEnableConfigHandler creates a handler from a http request which enables an integration via the integrationController
func (c *Controller) GetExportConfigHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// perform basic checks to ensure that the pipeline can be accessed
	fn := c.cloudCostChecks()
	if fn != nil {
		return fn
	}

	// Return valid handler func
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")

		integrationKey := r.URL.Query().Get("integrationKey")

		configs, err := c.ExportConfigs(integrationKey)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		protocol.WriteDataWithMessage(w, configs, "Configurations have been sanitized to protect secrets")
	}
}

// GetEnableConfigHandler creates a handler from a http request which enables an integration via the integrationController
func (c *Controller) GetAddConfigHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// perform basic checks to ensure that the pipeline can be accessed
	fn := c.cloudCostChecks()
	if fn != nil {
		return fn
	}

	// Return valid handler func
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")

		configType := r.URL.Query().Get("type")

		config, err := ParseConfig(configType, r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err = c.CreateConfig(config)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		protocol.WriteData(w, fmt.Sprintf("Successfully added integration with key %s", config.Key()))
	}
}

// GetEnableConfigHandler creates a handler from a http request which enables an integration via the integrationController
func (c *Controller) GetEnableConfigHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// perform basic checks to ensure that the pipeline can be accessed
	fn := c.cloudCostChecks()
	if fn != nil {
		return fn
	}

	// Return valid handler func
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")

		integrationKey := r.URL.Query().Get("integrationKey")
		if integrationKey == "" {
			http.Error(w, "required parameter 'integrationKey' is missing", http.StatusBadRequest)
			return
		}

		source := r.URL.Query().Get("source")
		if source == "" {
			http.Error(w, "required parameter 'source' is missing", http.StatusBadRequest)
			return
		}

		err := c.EnableConfig(integrationKey, source)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		protocol.WriteData(w, fmt.Sprintf("Successfully enabled integration with key %s from source %s", integrationKey, source))
	}
}

// GetDisableConfigHandler creates a handler from a http request which disables an integration via the integrationController
func (c *Controller) GetDisableConfigHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// perform basic checks to ensure that the pipeline can be accessed
	fn := c.cloudCostChecks()
	if fn != nil {
		return fn
	}

	// Return valid handler func
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")

		integrationKey := r.URL.Query().Get("integrationKey")
		if integrationKey == "" {
			http.Error(w, "required parameter 'integrationKey' is missing", http.StatusBadRequest)
			return
		}

		source := r.URL.Query().Get("source")
		if source == "" {
			http.Error(w, "required parameter 'source' is missing", http.StatusBadRequest)
			return
		}

		err := c.DisableConfig(integrationKey, source)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		protocol.WriteData(w, fmt.Sprintf("Successfully disabled integration with key %s from source %s", integrationKey, source))
	}
}

// GetDeleteConfigHandler creates a handler from a http request which deletes an integration via the integrationController
// if there are no other integrations with the given integration key, it also clears the data.
func (c *Controller) GetDeleteConfigHandler() func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// perform basic checks to ensure that the pipeline can be accessed
	fn := c.cloudCostChecks()
	if fn != nil {
		return fn
	}

	// Return valid handler func
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")

		integrationKey := r.URL.Query().Get("integrationKey")
		if integrationKey == "" {
			http.Error(w, "required parameter 'integrationKey' is missing", http.StatusBadRequest)
			return
		}

		err := c.DeleteConfig(integrationKey, ConfigControllerSource.String())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		protocol.WriteData(w, fmt.Sprintf("Successfully deleted integration with key %s", integrationKey))
	}
}
