package storage

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/httputil"
	"github.com/opencost/opencost/core/pkg/util/json"
	"gopkg.in/yaml.v2"
)

var defaultClusterConfig = ClusterConfig{
	Host: "localhost",
	Port: 9006,
	HTTPConfig: HTTPConfig{
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 2 * time.Minute,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		MaxConnsPerHost:       0,
	},
}

// ClusterStorage is a Storage implementation which connects to a remote file storage over http
type ClusterStorage struct {
	client *http.Client
	host   string
	port   int
}

type ClusterConfig struct {
	Host       string     `yaml:"host"`
	Port       int        `yaml:"port"`
	HTTPConfig HTTPConfig `yaml:"http_config"`
}

// parseConfig unmarshals a buffer into a Config with default HTTPConfig values.
func parseClusterConfig(conf []byte) (ClusterConfig, error) {
	config := defaultClusterConfig
	if err := yaml.Unmarshal(conf, &config); err != nil {
		return ClusterConfig{}, err
	}

	return config, nil
}

func NewClusterStorage(conf []byte) (*ClusterStorage, error) {
	config, err := parseClusterConfig(conf)
	if err != nil {
		return nil, err
	}

	return NewClusterStorageWith(config)
}

// NewBucketWithConfig returns a new Bucket using the provided s3 config values.
func NewClusterStorageWith(config ClusterConfig) (*ClusterStorage, error) {
	dt, err := config.HTTPConfig.GetHTTPTransport()
	if err != nil {
		return nil, fmt.Errorf("error creating transport: %w", err)
	}

	cs := &ClusterStorage{
		host:   config.Host,
		port:   config.Port,
		client: &http.Client{Transport: dt},
	}

	// Wait on cluster storage to respond before returning
	defaultWait := 5 * time.Second
	retry := 0
	maxTries := 5
	for {
		err := cs.check()
		if err == nil {
			break
		}

		log.Debugf("ClusterStorage: error connecting to cluster storage: %s", err.Error())
		if retry >= maxTries {
			return nil, fmt.Errorf("ClusterStorage: failed to connect to cluster storage after %d trys", maxTries)
		}
		waitTime := httputil.ExponentialBackoffWaitFor(defaultWait, retry)
		log.Infof("ClusterStorage: failed to connecting cluster storage. retry in %s", waitTime.String())
		time.Sleep(waitTime)
		retry++
	}

	return cs, nil
}

func (c *ClusterStorage) makeRequest(method, url string, body io.Reader, fn func(*http.Response) error) error {
	request, err := http.NewRequest(method, url, body)
	if err != nil {
		return fmt.Errorf("failed to build request: %w", err)
	}

	resp, err := c.client.Do(request)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}

	if !(resp.StatusCode >= 200 && resp.StatusCode <= 299) {
		if resp.Body != nil {
			var errResp Response[any]
			err = json.NewDecoder(resp.Body).Decode(&errResp)
			if err == nil {
				return fmt.Errorf("invalid response %d: %s", resp.StatusCode, errResp.Message)
			}
		}
		return fmt.Errorf("invalid response %d", resp.StatusCode)
	}

	if fn != nil {
		err = fn(resp)
		if err != nil {
			return fmt.Errorf("failed to handle response: %w", err)
		}
	}
	return nil
}

func (c *ClusterStorage) getURL(subpath string, args map[string]string) string {
	pathElems := strings.Split(subpath, "/")
	u := new(url.URL)
	u.Scheme = c.scheme()
	u.Host = net.JoinHostPort(c.host, fmt.Sprintf("%d", c.port))
	u = u.JoinPath(pathElems...)

	q := make(url.Values)
	for k, v := range args {
		q.Set(k, v)
	}

	rawQuery, _ := url.QueryUnescape(q.Encode())
	u.RawQuery = rawQuery

	return u.String() // <-- full URL string
}

func (c *ClusterStorage) check() error {
	err := c.makeRequest(
		http.MethodGet,
		c.getURL("healthz", nil),
		nil,
		nil,
	)
	if err != nil {
		return fmt.Errorf("ClusterStorage: failed health check: %w", err)
	}

	return nil
}

func (c *ClusterStorage) StorageType() StorageType {
	return StorageTypeCluster
}

func (c *ClusterStorage) scheme() string {
	if c.client.Transport != nil {
		if transport, ok := c.client.Transport.(*http.Transport); ok {
			if transport.TLSClientConfig != nil && !transport.TLSClientConfig.InsecureSkipVerify {
				return "https"
			}
		}
	}
	return "http"
}

func (c *ClusterStorage) FullPath(path string) string {
	var jsonResp Response[string]
	fn := func(resp *http.Response) error {
		err := json.NewDecoder(resp.Body).Decode(&jsonResp)
		if err != nil {
			return fmt.Errorf("failed to decode json: %w", err)
		}
		return nil
	}

	args := map[string]string{
		"path": path,
	}

	err := c.makeRequest(
		http.MethodGet,
		c.getURL("clusterStorage/fullPath", args),
		nil,
		fn,
	)
	if err != nil {
		log.Errorf("ClusterStorage: FullPath: %s", err.Error())
	}

	return jsonResp.Data
}

type Response[T any] struct {
	Code    int    `json:"code"`
	Data    T      `json:"data"`
	Message string `json:"message"`
}

func (c *ClusterStorage) Stat(path string) (*StorageInfo, error) {
	var jsonResp Response[*StorageInfo]
	fn := func(resp *http.Response) error {
		err := json.NewDecoder(resp.Body).Decode(&jsonResp)
		if err != nil {
			return fmt.Errorf("failed to decode json: %w", err)
		}
		return nil
	}

	args := map[string]string{
		"path": path,
	}

	err := c.makeRequest(
		http.MethodGet,
		c.getURL("clusterStorage/stat", args),
		nil,
		fn,
	)
	if err != nil {
		return nil, fmt.Errorf("ClusterStorage: Stat: %w", err)
	}

	return jsonResp.Data, nil
}

func (c *ClusterStorage) Read(path string) ([]byte, error) {
	var jsonResp Response[[]byte]
	fn := func(resp *http.Response) error {
		err := json.NewDecoder(resp.Body).Decode(&jsonResp)
		if err != nil {
			return fmt.Errorf("failed to decode json: %w", err)
		}
		return nil
	}

	args := map[string]string{
		"path": path,
	}

	err := c.makeRequest(
		http.MethodGet,
		c.getURL("clusterStorage/read", args),
		nil,
		fn,
	)
	if err != nil {
		return nil, fmt.Errorf("ClusterStorage: Read: %w", err)
	}

	return jsonResp.Data, nil
}

func (c *ClusterStorage) Write(path string, data []byte) error {
	fn := func(resp *http.Response) error {
		return nil
	}

	args := map[string]string{
		"path": path,
	}

	err := c.makeRequest(
		http.MethodPut,
		c.getURL("clusterStorage/write", args),
		bytes.NewReader(data),
		fn,
	)
	if err != nil {
		return fmt.Errorf("ClusterStorage: Write: %w", err)
	}

	return nil
}

func (c *ClusterStorage) Remove(path string) error {
	fn := func(resp *http.Response) error {
		return nil
	}

	args := map[string]string{
		"path": path,
	}

	err := c.makeRequest(
		http.MethodDelete,
		c.getURL("clusterStorage/remove", args),
		nil,
		fn,
	)
	if err != nil {
		return fmt.Errorf("ClusterStorage: Write: %w", err)
	}

	return nil
}

func (c *ClusterStorage) Exists(path string) (bool, error) {
	var jsonResp Response[bool]
	fn := func(resp *http.Response) error {
		err := json.NewDecoder(resp.Body).Decode(&jsonResp)
		if err != nil {
			return fmt.Errorf("failed to decode json: %w", err)
		}
		return nil
	}

	args := map[string]string{
		"path": path,
	}

	err := c.makeRequest(
		http.MethodGet,
		c.getURL("clusterStorage/exists", args),
		nil,
		fn,
	)
	if err != nil {
		return false, fmt.Errorf("ClusterStorage: Exists: %w", err)
	}

	return jsonResp.Data, nil
}

func (c *ClusterStorage) List(path string) ([]*StorageInfo, error) {
	var jsonResp Response[[]*StorageInfo]
	fn := func(resp *http.Response) error {
		err := json.NewDecoder(resp.Body).Decode(&jsonResp)
		if err != nil {
			return fmt.Errorf("failed to decode json: %w", err)
		}
		return nil
	}

	args := map[string]string{
		"path": path,
	}

	err := c.makeRequest(
		http.MethodGet,
		c.getURL("clusterStorage/list", args),
		nil,
		fn,
	)
	if err != nil {
		return nil, fmt.Errorf("ClusterStorage: List: %w", err)
	}

	return jsonResp.Data, nil
}

func (c *ClusterStorage) ListDirectories(path string) ([]*StorageInfo, error) {
	var jsonResp Response[[]*StorageInfo]
	fn := func(resp *http.Response) error {
		err := json.NewDecoder(resp.Body).Decode(&jsonResp)
		if err != nil {
			return fmt.Errorf("failed to decode json: %w", err)
		}
		return nil
	}

	args := map[string]string{
		"path": path,
	}

	err := c.makeRequest(
		http.MethodGet,
		c.getURL("clusterStorage/listDirectories", args),
		nil,
		fn,
	)
	if err != nil {
		return nil, fmt.Errorf("ClusterStorage: List Directories: %w", err)
	}

	// add '/' to the end of directory names to match other bucket storage types
	for _, info := range jsonResp.Data {
		info.Name = strings.TrimSuffix(info.Name, DirDelim) + DirDelim
	}

	return jsonResp.Data, nil
}
