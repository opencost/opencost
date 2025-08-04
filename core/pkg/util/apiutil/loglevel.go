package apiutil

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/opencost/opencost/core/pkg/log"
)

type LogLevelRequestResponse struct {
	Level string `json:"level"`
}

func GetLogLevel(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	level := log.GetLogLevel()
	llrr := LogLevelRequestResponse{
		Level: level,
	}

	body, err := json.Marshal(llrr)
	if err != nil {
		http.Error(w, fmt.Sprintf("unable to retrive log level"), http.StatusInternalServerError)
		return
	}
	_, err = w.Write(body)
	if err != nil {
		http.Error(w, fmt.Sprintf("unable to write response: %s", body), http.StatusInternalServerError)
		return
	}
}

func SetLogLevel(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	params := LogLevelRequestResponse{}
	err := json.NewDecoder(r.Body).Decode(&params)
	if err != nil {
		http.Error(w, fmt.Sprintf("unable to decode request body, error: %s", err), http.StatusBadRequest)
		return
	}

	err = log.SetLogLevel(params.Level)
	if err != nil {
		http.Error(w, fmt.Sprintf("level must be a valid log level according to zerolog; level given: %s, error: %s", params.Level, err), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}
