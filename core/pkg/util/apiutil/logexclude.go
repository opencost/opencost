package apiutil

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/log"
)

type LogExcludeRequestResponse struct {
	Patterns []string `json:"patterns"`
}

func GetLogExclude(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	exclusionPatterns := log.GetExcludePatterns()
	lerr := LogExcludeRequestResponse{
		Patterns: exclusionPatterns,
	}

	body, err := json.Marshal(lerr)
	if err != nil {
		http.Error(w, "unable to retrive log exclude", http.StatusInternalServerError)
		return
	}
	_, err = w.Write(body)
	if err != nil {
		http.Error(w, fmt.Sprintf("unable to write response: %s", body), http.StatusInternalServerError)
		return
	}
}

func SetLogExclude(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	params := LogExcludeRequestResponse{}
	err := json.NewDecoder(r.Body).Decode(&params)
	if err != nil {
		http.Error(w, fmt.Sprintf("unable to decode request body, error: %s", err), http.StatusBadRequest)
		return
	}

	log.SetExcludePatterns(params.Patterns)
	w.WriteHeader(http.StatusOK)
}
