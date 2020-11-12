package prom

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	prometheus "github.com/prometheus/client_golang/api"

	"github.com/kubecost/cost-model/pkg/errors"
	"github.com/kubecost/cost-model/pkg/log"
)

const (
	epRules = apiPrefix + "/rules"
)

// PromRecordingRule is the model used to represent a prometheus recording rule.
type PromRecordingRule struct {
	Name           string  `json:"name"`
	Query          string  `json:"query"`
	Health         string  `json:"health"`
	EvaluationTime float64 `json:"evaluationTime"`
	LastEvaluation string  `json:"lastEvaluation"`
	Type           string  `json:"type"`
}

// PromRuleGroup is the model used to represent a group of prometheus recording rules.
type PromRuleGroup struct {
	Name  string               `json:"name"`
	File  string               `json:"file"`
	Rules []*PromRecordingRule `json:"rules"`
}

// PromRuleData is the model used to represent the payload on a successful rule query.
type PromRuleData struct {
	Groups         []*PromRuleGroup `json:"groups"`
	Interval       int              `json:"interval"`
	EvaluationTime float64          `json:"evaluationTime"`
	LastEvaluation string           `json:"lastEvaluationTime"`
}

// PromRules is the model used to represent the prometheus response from the rules endpoint.
type PromRules struct {
	Data   *PromRuleData `json:"data"`
	Status string        `json:"status"`
}

var (
	recordingRulesLock sync.RWMutex
	recordingRules     map[string]*PromRecordingRule = make(map[string]*PromRecordingRule)
)

// LoadRecordingRules queries prometheus for the recording rules configured, and then caches
// the results locally by name
func LoadRecordingRules(cli prometheus.Client) bool {
	rulesCh := make(chan *PromRules)

	// query the recording rules on new go routine
	go func() {
		defer errors.HandlePanic()

		results, err := rules(cli)
		if err != nil {
			log.Errorf("%s", err)
		}

		rulesCh <- results
	}()

	rules := <-rulesCh
	if rules == nil {
		return false
	}

	if rules.Data == nil {
		return false
	}

	// Create a by name mapping
	ruleMap := make(map[string]*PromRecordingRule)
	for _, group := range rules.Data.Groups {
		for _, rule := range group.Rules {
			ruleMap[rule.Name] = rule
		}
	}

	recordingRulesLock.Lock()
	recordingRules = ruleMap
	recordingRulesLock.Unlock()

	return true
}

// HasRecordingRule determines if there exists a recording rule using the name provided.
func HasRecordingRule(name string) bool {
	recordingRulesLock.RLock()
	defer recordingRulesLock.RUnlock()

	_, ok := recordingRules[name]
	return ok
}

// Rules executes a request to the prometheus backend to retrieve the recording rules. This
// request explicitly filters for rules of type=record, which omits alert rules.
func rules(cli prometheus.Client) (*PromRules, error) {
	u := cli.URL(epRules, nil)
	q := u.Query()
	q.Set("type", "record")
	u.RawQuery = q.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, body, warnings, err := cli.Do(context.Background(), req)
	for _, w := range warnings {
		log.Warningf("fetching recording rules: %s", w)
	}

	if err != nil {
		if resp == nil {
			return nil, fmt.Errorf("rules error: %s", err.Error())
		}

		return nil, fmt.Errorf("rules error %d: %s", resp.StatusCode, err.Error())
	}

	var rules PromRules
	err = json.Unmarshal(body, &rules)
	if err != nil {
		return nil, fmt.Errorf("rules error: %s", err.Error())
	}

	return &rules, nil
}
