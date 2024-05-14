package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/opencost/opencost/pkg/cloud"
	"github.com/opencost/opencost/pkg/cloud/aws"
	"github.com/opencost/opencost/pkg/cloud/azure"
	"github.com/opencost/opencost/pkg/cloud/gcp"
)

func ParseConfig(configType string, body io.Reader) (cloud.KeyedConfig, error) {
	buf := new(bytes.Buffer)
	_, err := buf.ReadFrom(body)
	if err != nil {
		return nil, fmt.Errorf("failed to read body: %w", err)
	}

	return ParseConfigBytes(configType, buf.Bytes())
}

func ParseConfigBytes(configType string, configBytes []byte) (cloud.KeyedConfig, error) {
	var config cloud.KeyedConfig
	var err error

	switch strings.ToLower(configType) {
	case S3ConfigType:
		config = &aws.S3Configuration{}
	case AthenaConfigType:
		config = &aws.AthenaConfiguration{}
	case BigQueryConfigType:
		config = &gcp.BigQueryConfiguration{}
	case AzureStorageConfigType:
		config = &azure.StorageConfiguration{}
	default:
		return nil, fmt.Errorf("provided config type was not recognized %s", configType)
	}

	err = json.Unmarshal(configBytes, config)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling configuration of type %s: %w", configType, err)
	}

	return config, nil
}

func ParseConfigString(configType string, configStr string) (cloud.KeyedConfig, error) {
	return ParseConfigBytes(configType, []byte(configStr))
}
