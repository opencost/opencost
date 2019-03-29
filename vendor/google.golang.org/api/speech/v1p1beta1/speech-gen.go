// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// AUTO-GENERATED CODE. DO NOT EDIT.

// Package speech provides access to the Cloud Speech API.
//
// This package is DEPRECATED. Use package cloud.google.com/go/speech/apiv1 instead.
//
// See https://cloud.google.com/speech-to-text/docs/quickstart-protocol
//
// Usage example:
//
//   import "google.golang.org/api/speech/v1p1beta1"
//   ...
//   speechService, err := speech.New(oauthHttpClient)
package speech // import "google.golang.org/api/speech/v1p1beta1"

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	gensupport "google.golang.org/api/gensupport"
	googleapi "google.golang.org/api/googleapi"
)

// Always reference these packages, just in case the auto-generated code
// below doesn't.
var _ = bytes.NewBuffer
var _ = strconv.Itoa
var _ = fmt.Sprintf
var _ = json.NewDecoder
var _ = io.Copy
var _ = url.Parse
var _ = gensupport.MarshalJSON
var _ = googleapi.Version
var _ = errors.New
var _ = strings.Replace
var _ = context.Canceled

const apiId = "speech:v1p1beta1"
const apiName = "speech"
const apiVersion = "v1p1beta1"
const basePath = "https://speech.googleapis.com/"

// OAuth2 scopes used by this API.
const (
	// View and manage your data across Google Cloud Platform services
	CloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"
)

func New(client *http.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	s := &Service{client: client, BasePath: basePath}
	s.Operations = NewOperationsService(s)
	s.Projects = NewProjectsService(s)
	s.Speech = NewSpeechService(s)
	return s, nil
}

type Service struct {
	client    *http.Client
	BasePath  string // API endpoint base URL
	UserAgent string // optional additional User-Agent fragment

	Operations *OperationsService

	Projects *ProjectsService

	Speech *SpeechService
}

func (s *Service) userAgent() string {
	if s.UserAgent == "" {
		return googleapi.UserAgent
	}
	return googleapi.UserAgent + " " + s.UserAgent
}

func NewOperationsService(s *Service) *OperationsService {
	rs := &OperationsService{s: s}
	return rs
}

type OperationsService struct {
	s *Service
}

func NewProjectsService(s *Service) *ProjectsService {
	rs := &ProjectsService{s: s}
	rs.Locations = NewProjectsLocationsService(s)
	return rs
}

type ProjectsService struct {
	s *Service

	Locations *ProjectsLocationsService
}

func NewProjectsLocationsService(s *Service) *ProjectsLocationsService {
	rs := &ProjectsLocationsService{s: s}
	rs.Datasets = NewProjectsLocationsDatasetsService(s)
	rs.LogDataStats = NewProjectsLocationsLogDataStatsService(s)
	rs.Models = NewProjectsLocationsModelsService(s)
	return rs
}

type ProjectsLocationsService struct {
	s *Service

	Datasets *ProjectsLocationsDatasetsService

	LogDataStats *ProjectsLocationsLogDataStatsService

	Models *ProjectsLocationsModelsService
}

func NewProjectsLocationsDatasetsService(s *Service) *ProjectsLocationsDatasetsService {
	rs := &ProjectsLocationsDatasetsService{s: s}
	return rs
}

type ProjectsLocationsDatasetsService struct {
	s *Service
}

func NewProjectsLocationsLogDataStatsService(s *Service) *ProjectsLocationsLogDataStatsService {
	rs := &ProjectsLocationsLogDataStatsService{s: s}
	return rs
}

type ProjectsLocationsLogDataStatsService struct {
	s *Service
}

func NewProjectsLocationsModelsService(s *Service) *ProjectsLocationsModelsService {
	rs := &ProjectsLocationsModelsService{s: s}
	return rs
}

type ProjectsLocationsModelsService struct {
	s *Service
}

func NewSpeechService(s *Service) *SpeechService {
	rs := &SpeechService{s: s}
	return rs
}

type SpeechService struct {
	s *Service
}

// DataErrors: Different types of dataset errors and the stats
// associated with each error.
type DataErrors struct {
	// Count: Number of records having errors associated with the enum.
	Count int64 `json:"count,omitempty"`

	// ErrorType: Type of the error.
	//
	// Possible values:
	//   "ERROR_TYPE_UNSPECIFIED" - Not specified.
	//   "UNSUPPORTED_AUDIO_FORMAT" - Audio format not in the formats
	// supported by the cloud speech API
	//   "FILE_EXTENSION_MISMATCH_WITH_AUDIO_FORMAT" - File format different
	// from what is specified in the file name extension
	//   "FILE_TOO_LARGE" - File too large. Maximum allowed size is 50 MB.
	ErrorType string `json:"errorType,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Count") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Count") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DataErrors) MarshalJSON() ([]byte, error) {
	type NoMethod DataErrors
	raw := NoMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DataStats: Contains stats about the data which was uploaded and
// preprocessed to be
// use by downstream pipelines like training, evals pipelines.
type DataStats struct {
	// DataErrors: Different types of data errors and the counts associated
	// with them.
	DataErrors []*DataErrors `json:"dataErrors,omitempty"`

	// TestExampleCount: The number of examples used for testing.
	TestExampleCount int64 `json:"testExampleCount,omitempty"`

	// TrainingExampleCount: The number of examples used for training.
	TrainingExampleCount int64 `json:"trainingExampleCount,omitempty"`

	// ForceSendFields is a list of field names (e.g. "DataErrors") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "DataErrors") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *DataStats) MarshalJSON() ([]byte, error) {
	type NoMethod DataStats
	raw := NoMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Dataset: Specifies the parameters needed for creating a dataset. In
// addition this
// is also the message returned to the client by the `CreateDataset`
// method.
// It is included in the `result.response` field of the
// `Operation`
// returned by the `GetOperation` call of the
// `google::longrunning::Operations`
// service.
type Dataset struct {
	// BlockingOperationIds: Output only. All the blocking operations
	// associated with this dataset.
	// Like (pre-processing, training-model, testing-model)
	BlockingOperationIds []string `json:"blockingOperationIds,omitempty"`

	// BucketName: If set, the log data to be used in this dataset is
	// restricted to the
	// bucket specified. This field is only applicable if use_logged_data is
	// true.
	// If use_logged_data is true, but this field is not set, then all logs
	// will
	// be used for training the models. See: RecognitionMetadata for
	// information
	// on setting up data logs.
	BucketName string `json:"bucketName,omitempty"`

	// CreateTime: Output only. The timestamp this dataset is created.
	CreateTime string `json:"createTime,omitempty"`

	// DataProcessingRegion: Location where the data should be processed. If
	// not specified then we will
	// pick a location on behalf of the user for storing and processing the
	// data.
	// Currently only us-central is supported.
	DataProcessingRegion string `json:"dataProcessingRegion,omitempty"`

	// DataStats: Output only. Stats assoiated with the data.
	DataStats *DataStats `json:"dataStats,omitempty"`

	// DisplayName: Required. Name of the data set for display.
	DisplayName string `json:"displayName,omitempty"`

	// HasSufficientData: Output only. True if the data is sufficient to
	// create custom models.
	HasSufficientData bool `json:"hasSufficientData,omitempty"`

	// LanguageCode: Required. The language of the supplied audio as
	// a
	// [BCP-47](https://www.rfc-editor.org/rfc/bcp/bcp47.txt) language
	// tag.
	// Example: "en-US".
	// See [Language Support](/speech-to-text/docs/languages)
	// for a list of the currently supported language codes.
	LanguageCode string `json:"languageCode,omitempty"`

	// Models: All the models (including models pending training) built
	// using the dataset.
	Models []*Model `json:"models,omitempty"`

	// Name: Output only. Resource name of the dataset. Form
	// :-
	// '/projects/{project_number}/locations/{location_id}/datasets/{datas
	// et_id}'
	Name string `json:"name,omitempty"`

	// UpdateTime: Output only. The timestamp this dataset is last updated.
	UpdateTime string `json:"updateTime,omitempty"`

	// Uri: URI that points to a file in csv file where each row has
	// following
	// format.
	// <gs_path_to_audio>,<gs_path_to_transcript>,<label>
	// label can be HUMAN_TRANSCRIBED or MACHINE_TRANSCRIBED. To be valid,
	// rows
	// must do the following:
	// 1. Each row must have at least a label and <gs_path_to_transcript>
	// 2. If a row is marked HUMAN_TRANSCRIBED, then you must specify
	// both
	// <gs_path_to_audio> and <gs_path_to_transcript>. Only WAV file
	// formats
	// which encode linear 16-bit pulse-code modulation (PCM) audio format
	// are
	// supported. The maximum audio file size is 50 MB. Also note that the
	// audio
	// has to be single channel audio.
	// 3. There has to be at least 500 rows labelled HUMAN_TRANSCRIBED
	// covering
	// at least ~10K words in order to get reliable word error rate
	// results.
	// 4. To create a language model, you should provide at least 100,000
	// words
	// in your transcriptions as training data if you have conversational
	// and
	// captions type of data. You should provide at least 10,000 words if
	// you
	// have short utterances like voice commands and search type of use
	// cases.
	// Currently, only Google Cloud Storage URIs are
	// supported, which must be specified in the following
	// format:
	// `gs://bucket_name/object_name` (other URI formats will be
	// ignored).
	// For more information, see
	// [Request URIs](/storage/docs/reference-uris).
	Uri string `json:"uri,omitempty"`

	// UseLoggedData: If this is true, then use the previously logged data
	// (for the project)
	// The logs data for this project will be preprocessed and prepared
	// for
	// downstream pipelines (like training)
	UseLoggedData bool `json:"useLoggedData,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g.
	// "BlockingOperationIds") to unconditionally include in API requests.
	// By default, fields with empty values are omitted from API requests.
	// However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BlockingOperationIds") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *Dataset) MarshalJSON() ([]byte, error) {
	type NoMethod Dataset
	raw := NoMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// DeployModelRequest: Message sent by the client for the `DeployModel`
// method.
type DeployModelRequest struct {
}

// EvaluateModelRequest: Message sent by the client for the
// `EvaluateModel` method.
type EvaluateModelRequest struct {
}

// EvaluateModelResponse: The only message returned to the client by the
// `EvaluateModel` method. This
// is also returned as part of the Dataset message returned to the
// client by
// the CreateDataset method. It is included in the `result.response`
// field of
// the `Operation` returned by the `GetOperation` call of
// the
// `google::longrunning::Operations` service.
type EvaluateModelResponse struct {
	// IsEnhancedModel: If true then it means we are referring to the
	// results of an enhanced
	// version of the model_type. Currently only PHONE_CALL model_type has
	// an
	// enhanced version.
	IsEnhancedModel bool `json:"isEnhancedModel,omitempty"`

	// ModelType: Required. The type of model used in this evaluation.
	//
	// Possible values:
	//   "MODEL_TYPE_UNSPECIFIED"
	//   "DEFAULT" - Model for audio that is not one of the specific models
	// below. This is
	// a generic model and can be used in various scenarios but is
	// not
	// necessarily the best in any particular scenario.
	//   "COMMAND_AND_SEARCH" - Model for audio from short queries like
	// voice commands or voice search
	//   "PHONE_CALL" - Model for phone call conversation type op audio.
	//   "VIDEO" - Model for audio that originated from from video or
	// includes multiple
	// speakers.
	ModelType string `json:"modelType,omitempty"`

	// WordCount: Number of words used in the word_error_rate computation.
	WordCount int64 `json:"wordCount,omitempty"`

	// WordErrorRate: Word error rate metric computed on the test set using
	// the AutoML model.
	WordErrorRate float64 `json:"wordErrorRate,omitempty"`

	// ForceSendFields is a list of field names (e.g. "IsEnhancedModel") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "IsEnhancedModel") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *EvaluateModelResponse) MarshalJSON() ([]byte, error) {
	type NoMethod EvaluateModelResponse
	raw := NoMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

func (s *EvaluateModelResponse) UnmarshalJSON(data []byte) error {
	type NoMethod EvaluateModelResponse
	var s1 struct {
		WordErrorRate gensupport.JSONFloat64 `json:"wordErrorRate"`
		*NoMethod
	}
	s1.NoMethod = (*NoMethod)(s)
	if err := json.Unmarshal(data, &s1); err != nil {
		return err
	}
	s.WordErrorRate = float64(s1.WordErrorRate)
	return nil
}

type ListDatasetsResponse struct {
	// Datasets: Repeated list of data sets containing details about each
	// data set.
	Datasets []*Dataset `json:"datasets,omitempty"`

	// NextPageToken: Token to retrieve the next page of results, or empty
	// if there are no
	// more results in the list.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Datasets") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Datasets") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ListDatasetsResponse) MarshalJSON() ([]byte, error) {
	type NoMethod ListDatasetsResponse
	raw := NoMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// ListLogDataStatsResponse: Message received by the client for the
// `ListLogDataStats` method.
type ListLogDataStatsResponse struct {
	// LogDataEnabled: Output only. True if user has opted in for log data
	// collection.
	LogDataEnabled bool `json:"logDataEnabled,omitempty"`

	// LogDataStats: The stats for each bucket.
	LogDataStats []*LogBucketStats `json:"logDataStats,omitempty"`

	// TotalCount: The overall count for log data (including all bucket
	// data).
	TotalCount int64 `json:"totalCount,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "LogDataEnabled") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "LogDataEnabled") to
	// include in API requests with the JSON null value. By default, fields
	// with empty values are omitted from API requests. However, any field
	// with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *ListLogDataStatsResponse) MarshalJSON() ([]byte, error) {
	type NoMethod ListLogDataStatsResponse
	raw := NoMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

type ListModelsResponse struct {
	// Models: Repeated list of models containing details about each model.
	Models []*Model `json:"models,omitempty"`

	// NextPageToken: Token to retrieve the next page of results, or empty
	// if there are no
	// more results in the list.
	NextPageToken string `json:"nextPageToken,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Models") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Models") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *ListModelsResponse) MarshalJSON() ([]byte, error) {
	type NoMethod ListModelsResponse
	raw := NoMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// LogBucketStats: Stats for log data within a bucket.
type LogBucketStats struct {
	// BucketName: The display name for the bucket in which logs are
	// collected.
	BucketName string `json:"bucketName,omitempty"`

	// Count: Number of audio samples that have been collected in this
	// bucket.
	Count int64 `json:"count,omitempty"`

	// ForceSendFields is a list of field names (e.g. "BucketName") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "BucketName") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *LogBucketStats) MarshalJSON() ([]byte, error) {
	type NoMethod LogBucketStats
	raw := NoMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// LongRunningRecognizeRequest: The top-level message sent by the client
// for the `LongRunningRecognize`
// method.
type LongRunningRecognizeRequest struct {
	// Audio: *Required* The audio data to be recognized.
	Audio *RecognitionAudio `json:"audio,omitempty"`

	// Config: *Required* Provides information to the recognizer that
	// specifies how to
	// process the request.
	Config *RecognitionConfig `json:"config,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Audio") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Audio") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *LongRunningRecognizeRequest) MarshalJSON() ([]byte, error) {
	type NoMethod LongRunningRecognizeRequest
	raw := NoMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Model: Specifies the model parameters needed for training a model. In
// addition this
// is also the message returned to the client by the `CreateModel`
// method.
// It is included in the `result.response` field of the
// `Operation`
// returned by the `GetOperation` call of the
// `google::longrunning::Operations`
// service.
type Model struct {
	// CreateTime: Output only. Timestamp when this model was created.
	CreateTime string `json:"createTime,omitempty"`

	// DisplayName: Required. Display name of the model to be trained.
	DisplayName string `json:"displayName,omitempty"`

	// EvaluateModelResponses: Output only. Evaluation results associated
	// with this model. A model can
	// contain multiple sub-models in which case the evaluation results
	// for
	// all of those are available. If there are no sub models then there
	// would
	// be just a single EvaluateModelResponse.
	EvaluateModelResponses []*EvaluateModelResponse `json:"evaluateModelResponses,omitempty"`

	// Name: Output only. Resource name of the model.
	// Format:
	// "projects/{project_id}/locations/{location_id}/models/{model_id}"
	Name string `json:"name,omitempty"`

	// TrainingType: Required. Type of the training to perform.
	//
	// Possible values:
	//   "TRAINING_TYPE_UNSPECIFIED"
	//   "CUSTOM_ADAPTATION_LANGUAGE_MODEL" - Build adaptation language
	// model based on the users data. These models are
	// built on top of the existing prebuilt models (like phone_call,
	// video
	// etc.).
	//   "PREBUILT_MODEL" - Output only. This is set to indicate that the
	// model we are talking about
	// is a prebuilt model (for e.g in the context of evaluations).
	TrainingType string `json:"trainingType,omitempty"`

	// ForceSendFields is a list of field names (e.g. "CreateTime") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "CreateTime") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Model) MarshalJSON() ([]byte, error) {
	type NoMethod Model
	raw := NoMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Operation: This resource represents a long-running operation that is
// the result of a
// network API call.
type Operation struct {
	// Done: If the value is `false`, it means the operation is still in
	// progress.
	// If `true`, the operation is completed, and either `error` or
	// `response` is
	// available.
	Done bool `json:"done,omitempty"`

	// Error: The error result of the operation in case of failure or
	// cancellation.
	Error *Status `json:"error,omitempty"`

	// Metadata: Service-specific metadata associated with the operation.
	// It typically
	// contains progress information and common metadata such as create
	// time.
	// Some services might not provide such metadata.  Any method that
	// returns a
	// long-running operation should document the metadata type, if any.
	Metadata googleapi.RawMessage `json:"metadata,omitempty"`

	// Name: The server-assigned name, which is only unique within the same
	// service that
	// originally returns it. If you use the default HTTP mapping,
	// the
	// `name` should have the format of `operations/some/unique/name`.
	Name string `json:"name,omitempty"`

	// Response: The normal response of the operation in case of success.
	// If the original
	// method returns no data on success, such as `Delete`, the response
	// is
	// `google.protobuf.Empty`.  If the original method is
	// standard
	// `Get`/`Create`/`Update`, the response should be the resource.  For
	// other
	// methods, the response should have the type `XxxResponse`, where
	// `Xxx`
	// is the original method name.  For example, if the original method
	// name
	// is `TakeSnapshot()`, the inferred response type
	// is
	// `TakeSnapshotResponse`.
	Response googleapi.RawMessage `json:"response,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Done") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Done") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Operation) MarshalJSON() ([]byte, error) {
	type NoMethod Operation
	raw := NoMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RecognitionAudio: Contains audio data in the encoding specified in
// the `RecognitionConfig`.
// Either `content` or `uri` must be supplied. Supplying both or
// neither
// returns google.rpc.Code.INVALID_ARGUMENT. See
// [content limits](/speech-to-text/quotas#content).
type RecognitionAudio struct {
	// Content: The audio data bytes encoded as specified
	// in
	// `RecognitionConfig`. Note: as with all bytes fields, protobuffers use
	// a
	// pure binary representation, whereas JSON representations use base64.
	Content string `json:"content,omitempty"`

	// Uri: URI that points to a file that contains audio data bytes as
	// specified in
	// `RecognitionConfig`. The file must not be compressed (for example,
	// gzip).
	// Currently, only Google Cloud Storage URIs are
	// supported, which must be specified in the following
	// format:
	// `gs://bucket_name/object_name` (other URI formats
	// return
	// google.rpc.Code.INVALID_ARGUMENT). For more information, see
	// [Request URIs](https://cloud.google.com/storage/docs/reference-uris).
	Uri string `json:"uri,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Content") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Content") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RecognitionAudio) MarshalJSON() ([]byte, error) {
	type NoMethod RecognitionAudio
	raw := NoMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RecognitionConfig: Provides information to the recognizer that
// specifies how to process the
// request.
type RecognitionConfig struct {
	// AlternativeLanguageCodes: *Optional* A list of up to 3
	// additional
	// [BCP-47](https://www.rfc-editor.org/rfc/bcp/bcp47.txt) language
	// tags,
	// listing possible alternative languages of the supplied audio.
	// See [Language Support](/speech-to-text/docs/languages)
	// for a list of the currently supported language codes.
	// If alternative languages are listed, recognition result will
	// contain
	// recognition in the most likely language detected including the
	// main
	// language_code. The recognition result will include the language
	// tag
	// of the language detected in the audio.
	// Note: This feature is only supported for Voice Command and Voice
	// Search
	// use cases and performance may vary for other use cases (e.g., phone
	// call
	// transcription).
	AlternativeLanguageCodes []string `json:"alternativeLanguageCodes,omitempty"`

	// AudioChannelCount: *Optional* The number of channels in the input
	// audio data.
	// ONLY set this for MULTI-CHANNEL recognition.
	// Valid values for LINEAR16 and FLAC are `1`-`8`.
	// Valid values for OGG_OPUS are '1'-'254'.
	// Valid value for MULAW, AMR, AMR_WB and SPEEX_WITH_HEADER_BYTE is only
	// `1`.
	// If `0` or omitted, defaults to one channel (mono).
	// Note: We only recognize the first channel by default.
	// To perform independent recognition on each channel
	// set
	// `enable_separate_recognition_per_channel` to 'true'.
	AudioChannelCount int64 `json:"audioChannelCount,omitempty"`

	// DiarizationSpeakerCount: *Optional*
	// If set, specifies the estimated number of speakers in the
	// conversation.
	// If not set, defaults to '2'.
	// Ignored unless enable_speaker_diarization is set to true."
	DiarizationSpeakerCount int64 `json:"diarizationSpeakerCount,omitempty"`

	// EnableAutomaticPunctuation: *Optional* If 'true', adds punctuation to
	// recognition result hypotheses.
	// This feature is only available in select languages. Setting this
	// for
	// requests in other languages has no effect at all.
	// The default 'false' value does not add punctuation to result
	// hypotheses.
	// Note: This is currently offered as an experimental service,
	// complimentary
	// to all users. In the future this may be exclusively available as
	// a
	// premium feature.
	EnableAutomaticPunctuation bool `json:"enableAutomaticPunctuation,omitempty"`

	// EnableSeparateRecognitionPerChannel: This needs to be set to
	// ‘true’ explicitly and `audio_channel_count` > 1
	// to get each channel recognized separately. The recognition result
	// will
	// contain a `channel_tag` field to state which channel that result
	// belongs
	// to. If this is not true, we will only recognize the first channel.
	// The
	// request is billed cumulatively for all channels
	// recognized:
	// `audio_channel_count` multiplied by the length of the audio.
	EnableSeparateRecognitionPerChannel bool `json:"enableSeparateRecognitionPerChannel,omitempty"`

	// EnableSpeakerDiarization: *Optional* If 'true', enables speaker
	// detection for each recognized word in
	// the top alternative of the recognition result using a speaker_tag
	// provided
	// in the WordInfo.
	// Note: When this is true, we send all the words from the beginning of
	// the
	// audio for the top alternative in every consecutive STREAMING
	// responses.
	// This is done in order to improve our speaker tags as our models learn
	// to
	// identify the speakers in the conversation over time.
	// For non-streaming requests, the diarization results will be provided
	// only
	// in the top alternative of the FINAL SpeechRecognitionResult.
	EnableSpeakerDiarization bool `json:"enableSpeakerDiarization,omitempty"`

	// EnableWordConfidence: *Optional* If `true`, the top result includes a
	// list of words and the
	// confidence for those words. If `false`, no word-level
	// confidence
	// information is returned. The default is `false`.
	EnableWordConfidence bool `json:"enableWordConfidence,omitempty"`

	// EnableWordTimeOffsets: *Optional* If `true`, the top result includes
	// a list of words and
	// the start and end time offsets (timestamps) for those words.
	// If
	// `false`, no word-level time offset information is returned. The
	// default is
	// `false`.
	EnableWordTimeOffsets bool `json:"enableWordTimeOffsets,omitempty"`

	// Encoding: Encoding of audio data sent in all `RecognitionAudio`
	// messages.
	// This field is optional for `FLAC` and `WAV` audio files and
	// required
	// for all other audio formats. For details, see AudioEncoding.
	//
	// Possible values:
	//   "ENCODING_UNSPECIFIED" - Not specified.
	//   "LINEAR16" - Uncompressed 16-bit signed little-endian samples
	// (Linear PCM).
	//   "FLAC" - `FLAC` (Free Lossless Audio
	// Codec) is the recommended encoding because it is
	// lossless--therefore recognition is not compromised--and
	// requires only about half the bandwidth of `LINEAR16`. `FLAC`
	// stream
	// encoding supports 16-bit and 24-bit samples, however, not all fields
	// in
	// `STREAMINFO` are supported.
	//   "MULAW" - 8-bit samples that compand 14-bit audio samples using
	// G.711 PCMU/mu-law.
	//   "AMR" - Adaptive Multi-Rate Narrowband codec. `sample_rate_hertz`
	// must be 8000.
	//   "AMR_WB" - Adaptive Multi-Rate Wideband codec. `sample_rate_hertz`
	// must be 16000.
	//   "OGG_OPUS" - Opus encoded audio frames in Ogg
	// container
	// ([OggOpus](https://wiki.xiph.org/OggOpus)).
	// `sample_rate_her
	// tz` must be one of 8000, 12000, 16000, 24000, or 48000.
	//   "SPEEX_WITH_HEADER_BYTE" - Although the use of lossy encodings is
	// not recommended, if a very low
	// bitrate encoding is required, `OGG_OPUS` is highly preferred
	// over
	// Speex encoding. The [Speex](https://speex.org/)  encoding supported
	// by
	// Cloud Speech API has a header byte in each block, as in MIME
	// type
	// `audio/x-speex-with-header-byte`.
	// It is a variant of the RTP Speex encoding defined in
	// [RFC 5574](https://tools.ietf.org/html/rfc5574).
	// The stream is a sequence of blocks, one block per RTP packet. Each
	// block
	// starts with a byte containing the length of the block, in bytes,
	// followed
	// by one or more frames of Speex data, padded to an integral number
	// of
	// bytes (octets) as specified in RFC 5574. In other words, each RTP
	// header
	// is replaced with a single byte containing the block length. Only
	// Speex
	// wideband is supported. `sample_rate_hertz` must be 16000.
	Encoding string `json:"encoding,omitempty"`

	// LanguageCode: *Required* The language of the supplied audio as
	// a
	// [BCP-47](https://www.rfc-editor.org/rfc/bcp/bcp47.txt) language
	// tag.
	// Example: "en-US".
	// See [Language Support](/speech-to-text/docs/languages)
	// for a list of the currently supported language codes.
	LanguageCode string `json:"languageCode,omitempty"`

	// MaxAlternatives: *Optional* Maximum number of recognition hypotheses
	// to be returned.
	// Specifically, the maximum number of `SpeechRecognitionAlternative`
	// messages
	// within each `SpeechRecognitionResult`.
	// The server may return fewer than `max_alternatives`.
	// Valid values are `0`-`30`. A value of `0` or `1` will return a
	// maximum of
	// one. If omitted, will return a maximum of one.
	MaxAlternatives int64 `json:"maxAlternatives,omitempty"`

	// Metadata: *Optional* Metadata regarding this request.
	Metadata *RecognitionMetadata `json:"metadata,omitempty"`

	// Model: *Optional* Which model to select for the given request. Select
	// the model
	// best suited to your domain to get best results. If a model is
	// not
	// explicitly specified, then we auto-select a model based on the
	// parameters
	// in the RecognitionConfig.
	// <table>
	//   <tr>
	//     <td><b>Model</b></td>
	//     <td><b>Description</b></td>
	//   </tr>
	//   <tr>
	//     <td><code>command_and_search</code></td>
	//     <td>Best for short queries such as voice commands or voice
	// search.</td>
	//   </tr>
	//   <tr>
	//     <td><code>phone_call</code></td>
	//     <td>Best for audio that originated from a phone call (typically
	//     recorded at an 8khz sampling rate).</td>
	//   </tr>
	//   <tr>
	//     <td><code>video</code></td>
	//     <td>Best for audio that originated from from video or includes
	// multiple
	//         speakers. Ideally the audio is recorded at a 16khz or
	// greater
	//         sampling rate. This is a premium model that costs more than
	// the
	//         standard rate.</td>
	//   </tr>
	//   <tr>
	//     <td><code>default</code></td>
	//     <td>Best for audio that is not one of the specific audio models.
	//         For example, long-form audio. Ideally the audio is
	// high-fidelity,
	//         recorded at a 16khz or greater sampling rate.</td>
	//   </tr>
	// </table>
	Model string `json:"model,omitempty"`

	// ProfanityFilter: *Optional* If set to `true`, the server will attempt
	// to filter out
	// profanities, replacing all but the initial character in each filtered
	// word
	// with asterisks, e.g. "f***". If set to `false` or omitted,
	// profanities
	// won't be filtered out.
	ProfanityFilter bool `json:"profanityFilter,omitempty"`

	// SampleRateHertz: Sample rate in Hertz of the audio data sent in
	// all
	// `RecognitionAudio` messages. Valid values are: 8000-48000.
	// 16000 is optimal. For best results, set the sampling rate of the
	// audio
	// source to 16000 Hz. If that's not possible, use the native sample
	// rate of
	// the audio source (instead of re-sampling).
	// This field is optional for `FLAC` and `WAV` audio files and
	// required
	// for all other audio formats. For details, see AudioEncoding.
	SampleRateHertz int64 `json:"sampleRateHertz,omitempty"`

	// SpeechContexts: *Optional* array of SpeechContext.
	// A means to provide context to assist the speech recognition. For
	// more
	// information, see [Phrase
	// Hints](/speech-to-text/docs/basics#phrase-hints).
	SpeechContexts []*SpeechContext `json:"speechContexts,omitempty"`

	// UseEnhanced: *Optional* Set to true to use an enhanced model for
	// speech recognition.
	// If `use_enhanced` is set to true and the `model` field is not set,
	// then
	// an appropriate enhanced model is chosen if:
	// 1. project is eligible for requesting enhanced models
	// 2. an enhanced model exists for the audio
	//
	// If `use_enhanced` is true and an enhanced version of the specified
	// model
	// does not exist, then the speech is recognized using the standard
	// version
	// of the specified model.
	//
	// Enhanced speech models require that you opt-in to data logging
	// using
	// instructions in
	// the
	// [documentation](/speech-to-text/docs/enable-data-logging). If you
	// set
	// `use_enhanced` to true and you have not enabled audio logging, then
	// you
	// will receive an error.
	UseEnhanced bool `json:"useEnhanced,omitempty"`

	// ForceSendFields is a list of field names (e.g.
	// "AlternativeLanguageCodes") to unconditionally include in API
	// requests. By default, fields with empty values are omitted from API
	// requests. However, any non-pointer, non-interface field appearing in
	// ForceSendFields will be sent to the server regardless of whether the
	// field is empty or not. This may be used to include empty fields in
	// Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AlternativeLanguageCodes")
	// to include in API requests with the JSON null value. By default,
	// fields with empty values are omitted from API requests. However, any
	// field with an empty value appearing in NullFields will be sent to the
	// server as null. It is an error if a field in this list has a
	// non-empty value. This may be used to include null fields in Patch
	// requests.
	NullFields []string `json:"-"`
}

func (s *RecognitionConfig) MarshalJSON() ([]byte, error) {
	type NoMethod RecognitionConfig
	raw := NoMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RecognitionMetadata: Description of audio data to be recognized.
type RecognitionMetadata struct {
	// AudioTopic: Description of the content. Eg. "Recordings of federal
	// supreme court
	// hearings from 2012".
	AudioTopic string `json:"audioTopic,omitempty"`

	// IndustryNaicsCodeOfAudio: The industry vertical to which this speech
	// recognition request most
	// closely applies. This is most indicative of the topics contained
	// in the audio.  Use the 6-digit NAICS code to identify the
	// industry
	// vertical - see https://www.naics.com/search/.
	IndustryNaicsCodeOfAudio int64 `json:"industryNaicsCodeOfAudio,omitempty"`

	// InteractionType: The use case most closely describing the audio
	// content to be recognized.
	//
	// Possible values:
	//   "INTERACTION_TYPE_UNSPECIFIED" - Use case is either unknown or is
	// something other than one of the other
	// values below.
	//   "DISCUSSION" - Multiple people in a conversation or discussion. For
	// example in a
	// meeting with two or more people actively participating. Typically
	// all the primary people speaking would be in the same room (if
	// not,
	// see PHONE_CALL)
	//   "PRESENTATION" - One or more persons lecturing or presenting to
	// others, mostly
	// uninterrupted.
	//   "PHONE_CALL" - A phone-call or video-conference in which two or
	// more people, who are
	// not in the same room, are actively participating.
	//   "VOICEMAIL" - A recorded message intended for another person to
	// listen to.
	//   "PROFESSIONALLY_PRODUCED" - Professionally produced audio (eg. TV
	// Show, Podcast).
	//   "VOICE_SEARCH" - Transcribe spoken questions and queries into text.
	//   "VOICE_COMMAND" - Transcribe voice commands, such as for
	// controlling a device.
	//   "DICTATION" - Transcribe speech to text to create a written
	// document, such as a
	// text-message, email or report.
	InteractionType string `json:"interactionType,omitempty"`

	// MicrophoneDistance: The audio type that most closely describes the
	// audio being recognized.
	//
	// Possible values:
	//   "MICROPHONE_DISTANCE_UNSPECIFIED" - Audio type is not known.
	//   "NEARFIELD" - The audio was captured from a closely placed
	// microphone. Eg. phone,
	// dictaphone, or handheld microphone. Generally if there speaker is
	// within
	// 1 meter of the microphone.
	//   "MIDFIELD" - The speaker if within 3 meters of the microphone.
	//   "FARFIELD" - The speaker is more than 3 meters away from the
	// microphone.
	MicrophoneDistance string `json:"microphoneDistance,omitempty"`

	// ObfuscatedId: Obfuscated (privacy-protected) ID of the user, to
	// identify number of
	// unique users using the service.
	ObfuscatedId int64 `json:"obfuscatedId,omitempty,string"`

	// OriginalMediaType: The original media the speech was recorded on.
	//
	// Possible values:
	//   "ORIGINAL_MEDIA_TYPE_UNSPECIFIED" - Unknown original media type.
	//   "AUDIO" - The speech data is an audio recording.
	//   "VIDEO" - The speech data originally recorded on a video.
	OriginalMediaType string `json:"originalMediaType,omitempty"`

	// OriginalMimeType: Mime type of the original audio file.  For example
	// `audio/m4a`,
	// `audio/x-alaw-basic`, `audio/mp3`, `audio/3gpp`.
	// A list of possible audio mime types is maintained
	// at
	// http://www.iana.org/assignments/media-types/media-types.xhtml#audio
	OriginalMimeType string `json:"originalMimeType,omitempty"`

	// RecordingDeviceName: The device used to make the recording.  Examples
	// 'Nexus 5X' or
	// 'Polycom SoundStation IP 6000' or 'POTS' or 'VoIP' or
	// 'Cardioid Microphone'.
	RecordingDeviceName string `json:"recordingDeviceName,omitempty"`

	// RecordingDeviceType: The type of device the speech was recorded with.
	//
	// Possible values:
	//   "RECORDING_DEVICE_TYPE_UNSPECIFIED" - The recording device is
	// unknown.
	//   "SMARTPHONE" - Speech was recorded on a smartphone.
	//   "PC" - Speech was recorded using a personal computer or tablet.
	//   "PHONE_LINE" - Speech was recorded over a phone line.
	//   "VEHICLE" - Speech was recorded in a vehicle.
	//   "OTHER_OUTDOOR_DEVICE" - Speech was recorded outdoors.
	//   "OTHER_INDOOR_DEVICE" - Speech was recorded indoors.
	RecordingDeviceType string `json:"recordingDeviceType,omitempty"`

	// Tags: A freeform field to tag this input sample with. This can be
	// used for
	// grouping the logs into separate buckets. This enables selective
	// purging of
	// data based on the tags, and also for training models in AutoML.
	Tags []string `json:"tags,omitempty"`

	// ForceSendFields is a list of field names (e.g. "AudioTopic") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "AudioTopic") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RecognitionMetadata) MarshalJSON() ([]byte, error) {
	type NoMethod RecognitionMetadata
	raw := NoMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RecognizeRequest: The top-level message sent by the client for the
// `Recognize` method.
type RecognizeRequest struct {
	// Audio: *Required* The audio data to be recognized.
	Audio *RecognitionAudio `json:"audio,omitempty"`

	// Config: *Required* Provides information to the recognizer that
	// specifies how to
	// process the request.
	Config *RecognitionConfig `json:"config,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Audio") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Audio") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RecognizeRequest) MarshalJSON() ([]byte, error) {
	type NoMethod RecognizeRequest
	raw := NoMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RecognizeResponse: The only message returned to the client by the
// `Recognize` method. It
// contains the result as zero or more sequential
// `SpeechRecognitionResult`
// messages.
type RecognizeResponse struct {
	// Results: Output only. Sequential list of transcription results
	// corresponding to
	// sequential portions of audio.
	Results []*SpeechRecognitionResult `json:"results,omitempty"`

	// ServerResponse contains the HTTP response code and headers from the
	// server.
	googleapi.ServerResponse `json:"-"`

	// ForceSendFields is a list of field names (e.g. "Results") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Results") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RecognizeResponse) MarshalJSON() ([]byte, error) {
	type NoMethod RecognizeResponse
	raw := NoMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// RefreshDataRequest: Message sent by the client to refresh data in a
// existing dataset.
type RefreshDataRequest struct {
	// Uri: URI that points to a file in csv file where each row has
	// following
	// format.
	// <gs_path_to_audio>,<gs_path_to_transcript>,<label>
	// label can be HUMAN_TRANSCRIBED or MACHINE_TRANSCRIBED. Few rules for
	// a row
	// to be considered valid are :-
	// 1. Each row must have at least a label and <gs_path_to_transcript>
	// 2. If a row is marked HUMAN_TRANSCRIBED, then both
	// <gs_path_to_audio>
	// and <gs_path_to_transcript> needs to be specified.
	// 3. There has to be minimum 500 number of rows labelled
	// HUMAN_TRANSCRIBED if
	// evaluation stats are required.
	// 4. If use_logged_data_for_training is set to true, then we ignore the
	// rows
	// labelled as MACHINE_TRANSCRIBED.
	// 5. There has to be minimum 100,000 words in the transcripts in order
	// to
	// provide sufficient textual training data for the language
	// model.
	// Currently, only Google Cloud Storage URIs are
	// supported, which must be specified in the following
	// format:
	// `gs://bucket_name/object_name` (other URI formats will be
	// ignored).
	// For more information, see
	// [Request URIs](https://cloud.google.com/storage/docs/reference-uris).
	Uri string `json:"uri,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Uri") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Uri") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *RefreshDataRequest) MarshalJSON() ([]byte, error) {
	type NoMethod RefreshDataRequest
	raw := NoMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SpeechContext: Provides "hints" to the speech recognizer to favor
// specific words and phrases
// in the results.
type SpeechContext struct {
	// Phrases: *Optional* A list of strings containing words and phrases
	// "hints" so that
	// the speech recognition is more likely to recognize them. This can be
	// used
	// to improve the accuracy for specific words and phrases, for example,
	// if
	// specific commands are typically spoken by the user. This can also be
	// used
	// to add additional words to the vocabulary of the recognizer.
	// See
	// [usage limits](/speech-to-text/quotas#content).
	Phrases []string `json:"phrases,omitempty"`

	// Strength: Hint strength to use (high, medium or low). If you use a
	// high strength then
	// you are more likely to see those phrases in the results. If strength
	// is not
	// specified then by default medium strength will be used. If you'd
	// like
	// different phrases to have different strengths, you can specify
	// multiple
	// speech_contexts.
	//
	// Possible values:
	//   "STRENGTH_UNSPECIFIED"
	//   "LOW" - Low strength
	//   "MEDIUM" - Medium strength
	//   "HIGH" - High strength
	Strength string `json:"strength,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Phrases") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Phrases") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SpeechContext) MarshalJSON() ([]byte, error) {
	type NoMethod SpeechContext
	raw := NoMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// SpeechRecognitionAlternative: Alternative hypotheses (a.k.a. n-best
// list).
type SpeechRecognitionAlternative struct {
	// Confidence: Output only. The confidence estimate between 0.0 and 1.0.
	// A higher number
	// indicates an estimated greater likelihood that the recognized words
	// are
	// correct. This field is set only for the top alternative of a
	// non-streaming
	// result or, of a streaming result where `is_final=true`.
	// This field is not guaranteed to be accurate and users should not rely
	// on it
	// to be always provided.
	// The default of 0.0 is a sentinel value indicating `confidence` was
	// not set.
	Confidence float64 `json:"confidence,omitempty"`

	// Transcript: Output only. Transcript text representing the words that
	// the user spoke.
	Transcript string `json:"transcript,omitempty"`

	// Words: Output only. A list of word-specific information for each
	// recognized word.
	// Note: When `enable_speaker_diarization` is true, you will see all the
	// words
	// from the beginning of the audio.
	Words []*WordInfo `json:"words,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Confidence") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Confidence") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SpeechRecognitionAlternative) MarshalJSON() ([]byte, error) {
	type NoMethod SpeechRecognitionAlternative
	raw := NoMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

func (s *SpeechRecognitionAlternative) UnmarshalJSON(data []byte) error {
	type NoMethod SpeechRecognitionAlternative
	var s1 struct {
		Confidence gensupport.JSONFloat64 `json:"confidence"`
		*NoMethod
	}
	s1.NoMethod = (*NoMethod)(s)
	if err := json.Unmarshal(data, &s1); err != nil {
		return err
	}
	s.Confidence = float64(s1.Confidence)
	return nil
}

// SpeechRecognitionResult: A speech recognition result corresponding to
// a portion of the audio.
type SpeechRecognitionResult struct {
	// Alternatives: Output only. May contain one or more recognition
	// hypotheses (up to the
	// maximum specified in `max_alternatives`).
	// These alternatives are ordered in terms of accuracy, with the top
	// (first)
	// alternative being the most probable, as ranked by the recognizer.
	Alternatives []*SpeechRecognitionAlternative `json:"alternatives,omitempty"`

	// ChannelTag: For multi-channel audio, this is the channel number
	// corresponding to the
	// recognized result for the audio from that channel.
	// For audio_channel_count = N, its output values can range from '1' to
	// 'N'.
	ChannelTag int64 `json:"channelTag,omitempty"`

	// LanguageCode: Output only.
	// The
	// [BCP-47](https://www.rfc-editor.org/rfc/bcp/bcp47.txt) language tag
	// of the
	// language in this result. This language code was detected to have the
	// most
	// likelihood of being spoken in the audio.
	LanguageCode string `json:"languageCode,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Alternatives") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Alternatives") to include
	// in API requests with the JSON null value. By default, fields with
	// empty values are omitted from API requests. However, any field with
	// an empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *SpeechRecognitionResult) MarshalJSON() ([]byte, error) {
	type NoMethod SpeechRecognitionResult
	raw := NoMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// Status: The `Status` type defines a logical error model that is
// suitable for different
// programming environments, including REST APIs and RPC APIs. It is
// used by
// [gRPC](https://github.com/grpc). The error model is designed to
// be:
//
// - Simple to use and understand for most users
// - Flexible enough to meet unexpected needs
//
// # Overview
//
// The `Status` message contains three pieces of data: error code, error
// message,
// and error details. The error code should be an enum value
// of
// google.rpc.Code, but it may accept additional error codes if needed.
// The
// error message should be a developer-facing English message that
// helps
// developers *understand* and *resolve* the error. If a localized
// user-facing
// error message is needed, put the localized message in the error
// details or
// localize it in the client. The optional error details may contain
// arbitrary
// information about the error. There is a predefined set of error
// detail types
// in the package `google.rpc` that can be used for common error
// conditions.
//
// # Language mapping
//
// The `Status` message is the logical representation of the error
// model, but it
// is not necessarily the actual wire format. When the `Status` message
// is
// exposed in different client libraries and different wire protocols,
// it can be
// mapped differently. For example, it will likely be mapped to some
// exceptions
// in Java, but more likely mapped to some error codes in C.
//
// # Other uses
//
// The error model and the `Status` message can be used in a variety
// of
// environments, either with or without APIs, to provide a
// consistent developer experience across different
// environments.
//
// Example uses of this error model include:
//
// - Partial errors. If a service needs to return partial errors to the
// client,
//     it may embed the `Status` in the normal response to indicate the
// partial
//     errors.
//
// - Workflow errors. A typical workflow has multiple steps. Each step
// may
//     have a `Status` message for error reporting.
//
// - Batch operations. If a client uses batch request and batch
// response, the
//     `Status` message should be used directly inside batch response,
// one for
//     each error sub-response.
//
// - Asynchronous operations. If an API call embeds asynchronous
// operation
//     results in its response, the status of those operations should
// be
//     represented directly using the `Status` message.
//
// - Logging. If some API errors are stored in logs, the message
// `Status` could
//     be used directly after any stripping needed for security/privacy
// reasons.
type Status struct {
	// Code: The status code, which should be an enum value of
	// google.rpc.Code.
	Code int64 `json:"code,omitempty"`

	// Details: A list of messages that carry the error details.  There is a
	// common set of
	// message types for APIs to use.
	Details []googleapi.RawMessage `json:"details,omitempty"`

	// Message: A developer-facing error message, which should be in
	// English. Any
	// user-facing error message should be localized and sent in
	// the
	// google.rpc.Status.details field, or localized by the client.
	Message string `json:"message,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Code") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Code") to include in API
	// requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *Status) MarshalJSON() ([]byte, error) {
	type NoMethod Status
	raw := NoMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

// WordInfo: Word-specific information for recognized words.
type WordInfo struct {
	// Confidence: Output only. The confidence estimate between 0.0 and 1.0.
	// A higher number
	// indicates an estimated greater likelihood that the recognized words
	// are
	// correct. This field is set only for the top alternative of a
	// non-streaming
	// result or, of a streaming result where `is_final=true`.
	// This field is not guaranteed to be accurate and users should not rely
	// on it
	// to be always provided.
	// The default of 0.0 is a sentinel value indicating `confidence` was
	// not set.
	Confidence float64 `json:"confidence,omitempty"`

	// EndTime: Output only. Time offset relative to the beginning of the
	// audio,
	// and corresponding to the end of the spoken word.
	// This field is only set if `enable_word_time_offsets=true` and only
	// in the top hypothesis.
	// This is an experimental feature and the accuracy of the time offset
	// can
	// vary.
	EndTime string `json:"endTime,omitempty"`

	// SpeakerTag: Output only. A distinct integer value is assigned for
	// every speaker within
	// the audio. This field specifies which one of those speakers was
	// detected to
	// have spoken this word. Value ranges from '1' to
	// diarization_speaker_count.
	// speaker_tag is set if enable_speaker_diarization = 'true' and only in
	// the
	// top alternative.
	SpeakerTag int64 `json:"speakerTag,omitempty"`

	// StartTime: Output only. Time offset relative to the beginning of the
	// audio,
	// and corresponding to the start of the spoken word.
	// This field is only set if `enable_word_time_offsets=true` and only
	// in the top hypothesis.
	// This is an experimental feature and the accuracy of the time offset
	// can
	// vary.
	StartTime string `json:"startTime,omitempty"`

	// Word: Output only. The word corresponding to this set of information.
	Word string `json:"word,omitempty"`

	// ForceSendFields is a list of field names (e.g. "Confidence") to
	// unconditionally include in API requests. By default, fields with
	// empty values are omitted from API requests. However, any non-pointer,
	// non-interface field appearing in ForceSendFields will be sent to the
	// server regardless of whether the field is empty or not. This may be
	// used to include empty fields in Patch requests.
	ForceSendFields []string `json:"-"`

	// NullFields is a list of field names (e.g. "Confidence") to include in
	// API requests with the JSON null value. By default, fields with empty
	// values are omitted from API requests. However, any field with an
	// empty value appearing in NullFields will be sent to the server as
	// null. It is an error if a field in this list has a non-empty value.
	// This may be used to include null fields in Patch requests.
	NullFields []string `json:"-"`
}

func (s *WordInfo) MarshalJSON() ([]byte, error) {
	type NoMethod WordInfo
	raw := NoMethod(*s)
	return gensupport.MarshalJSON(raw, s.ForceSendFields, s.NullFields)
}

func (s *WordInfo) UnmarshalJSON(data []byte) error {
	type NoMethod WordInfo
	var s1 struct {
		Confidence gensupport.JSONFloat64 `json:"confidence"`
		*NoMethod
	}
	s1.NoMethod = (*NoMethod)(s)
	if err := json.Unmarshal(data, &s1); err != nil {
		return err
	}
	s.Confidence = float64(s1.Confidence)
	return nil
}

// method id "speech.operations.get":

type OperationsGetCall struct {
	s            *Service
	name         string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Gets the latest state of a long-running operation.  Clients can
// use this
// method to poll the operation result at intervals as recommended by
// the API
// service.
func (r *OperationsService) Get(name string) *OperationsGetCall {
	c := &OperationsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *OperationsGetCall) Fields(s ...googleapi.Field) *OperationsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *OperationsGetCall) IfNoneMatch(entityTag string) *OperationsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *OperationsGetCall) Context(ctx context.Context) *OperationsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *OperationsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *OperationsGetCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	if c.ifNoneMatch_ != "" {
		reqHeaders.Set("If-None-Match", c.ifNoneMatch_)
	}
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	c.urlParams_.Set("prettyPrint", "false")
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1p1beta1/operations/{+name}")
	urls += "?" + c.urlParams_.Encode()
	req, err := http.NewRequest("GET", urls, body)
	if err != nil {
		return nil, err
	}
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "speech.operations.get" call.
// Exactly one of *Operation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Operation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *OperationsGetCall) Do(opts ...googleapi.CallOption) (*Operation, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Operation{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := gensupport.DecodeResponse(target, res); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Gets the latest state of a long-running operation.  Clients can use this\nmethod to poll the operation result at intervals as recommended by the API\nservice.",
	//   "flatPath": "v1p1beta1/operations/{operationsId}",
	//   "httpMethod": "GET",
	//   "id": "speech.operations.get",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "The name of the operation resource.",
	//       "location": "path",
	//       "pattern": "^[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1p1beta1/operations/{+name}",
	//   "response": {
	//     "$ref": "Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "speech.projects.locations.datasets.create":

type ProjectsLocationsDatasetsCreateCall struct {
	s          *Service
	parent     string
	dataset    *Dataset
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Create: Performs asynchronous data upload for AutoML: receive results
// via the
// google.longrunning.Operations interface. Returns either
// an
// `Operation.error` or an `Operation.response` which contains
// a `Dataset` message.
func (r *ProjectsLocationsDatasetsService) Create(parent string, dataset *Dataset) *ProjectsLocationsDatasetsCreateCall {
	c := &ProjectsLocationsDatasetsCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.parent = parent
	c.dataset = dataset
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsLocationsDatasetsCreateCall) Fields(s ...googleapi.Field) *ProjectsLocationsDatasetsCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsLocationsDatasetsCreateCall) Context(ctx context.Context) *ProjectsLocationsDatasetsCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsLocationsDatasetsCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsLocationsDatasetsCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.dataset)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	c.urlParams_.Set("prettyPrint", "false")
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1p1beta1/{+parent}/datasets")
	urls += "?" + c.urlParams_.Encode()
	req, err := http.NewRequest("POST", urls, body)
	if err != nil {
		return nil, err
	}
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"parent": c.parent,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "speech.projects.locations.datasets.create" call.
// Exactly one of *Operation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Operation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ProjectsLocationsDatasetsCreateCall) Do(opts ...googleapi.CallOption) (*Operation, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Operation{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := gensupport.DecodeResponse(target, res); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Performs asynchronous data upload for AutoML: receive results via the\ngoogle.longrunning.Operations interface. Returns either an\n`Operation.error` or an `Operation.response` which contains\na `Dataset` message.",
	//   "flatPath": "v1p1beta1/projects/{projectsId}/locations/{locationsId}/datasets",
	//   "httpMethod": "POST",
	//   "id": "speech.projects.locations.datasets.create",
	//   "parameterOrder": [
	//     "parent"
	//   ],
	//   "parameters": {
	//     "parent": {
	//       "description": "Required. Resource name of the parent. Has the format :-\n\"projects/{project_id}/locations/{location_id}\"",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/locations/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1p1beta1/{+parent}/datasets",
	//   "request": {
	//     "$ref": "Dataset"
	//   },
	//   "response": {
	//     "$ref": "Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "speech.projects.locations.datasets.get":

type ProjectsLocationsDatasetsGetCall struct {
	s            *Service
	name         string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// Get: Get the dataset associated with the dataset resource.
func (r *ProjectsLocationsDatasetsService) Get(name string) *ProjectsLocationsDatasetsGetCall {
	c := &ProjectsLocationsDatasetsGetCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	return c
}

// IncludeModelInfo sets the optional parameter "includeModelInfo": If
// true then also include information about the models built using
// this
// dataset.
func (c *ProjectsLocationsDatasetsGetCall) IncludeModelInfo(includeModelInfo bool) *ProjectsLocationsDatasetsGetCall {
	c.urlParams_.Set("includeModelInfo", fmt.Sprint(includeModelInfo))
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsLocationsDatasetsGetCall) Fields(s ...googleapi.Field) *ProjectsLocationsDatasetsGetCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsLocationsDatasetsGetCall) IfNoneMatch(entityTag string) *ProjectsLocationsDatasetsGetCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsLocationsDatasetsGetCall) Context(ctx context.Context) *ProjectsLocationsDatasetsGetCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsLocationsDatasetsGetCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsLocationsDatasetsGetCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	if c.ifNoneMatch_ != "" {
		reqHeaders.Set("If-None-Match", c.ifNoneMatch_)
	}
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	c.urlParams_.Set("prettyPrint", "false")
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1p1beta1/{+name}")
	urls += "?" + c.urlParams_.Encode()
	req, err := http.NewRequest("GET", urls, body)
	if err != nil {
		return nil, err
	}
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "speech.projects.locations.datasets.get" call.
// Exactly one of *Dataset or error will be non-nil. Any non-2xx status
// code is an error. Response headers are in either
// *Dataset.ServerResponse.Header or (if a response was returned at all)
// in error.(*googleapi.Error).Header. Use googleapi.IsNotModified to
// check whether the returned error was because http.StatusNotModified
// was returned.
func (c *ProjectsLocationsDatasetsGetCall) Do(opts ...googleapi.CallOption) (*Dataset, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Dataset{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := gensupport.DecodeResponse(target, res); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Get the dataset associated with the dataset resource.",
	//   "flatPath": "v1p1beta1/projects/{projectsId}/locations/{locationsId}/datasets/{datasetsId}",
	//   "httpMethod": "GET",
	//   "id": "speech.projects.locations.datasets.get",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "includeModelInfo": {
	//       "description": "If true then also include information about the models built using this\ndataset.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "name": {
	//       "description": "The resource name of the dataset to retrieve. Form :-\n'/projects/{project_number}/locations/{location_id}/datasets/{dataset_id}'",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/locations/[^/]+/datasets/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1p1beta1/{+name}",
	//   "response": {
	//     "$ref": "Dataset"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "speech.projects.locations.datasets.list":

type ProjectsLocationsDatasetsListCall struct {
	s            *Service
	parent       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Fetch the list of dataset associated with this project.
func (r *ProjectsLocationsDatasetsService) List(parent string) *ProjectsLocationsDatasetsListCall {
	c := &ProjectsLocationsDatasetsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.parent = parent
	return c
}

// Filter sets the optional parameter "filter": Filter the response
// based on display_name of the dataset. For e.g
// display_name=Foo The filter string is case sensitive
func (c *ProjectsLocationsDatasetsListCall) Filter(filter string) *ProjectsLocationsDatasetsListCall {
	c.urlParams_.Set("filter", filter)
	return c
}

// IncludeModelInfo sets the optional parameter "includeModelInfo": If
// true then also include information about the models built using
// the
// datasets.
func (c *ProjectsLocationsDatasetsListCall) IncludeModelInfo(includeModelInfo bool) *ProjectsLocationsDatasetsListCall {
	c.urlParams_.Set("includeModelInfo", fmt.Sprint(includeModelInfo))
	return c
}

// PageSize sets the optional parameter "pageSize": The maximum number
// of items to return.
func (c *ProjectsLocationsDatasetsListCall) PageSize(pageSize int64) *ProjectsLocationsDatasetsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": The
// next_page_token value returned from a previous List request, if any.
func (c *ProjectsLocationsDatasetsListCall) PageToken(pageToken string) *ProjectsLocationsDatasetsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsLocationsDatasetsListCall) Fields(s ...googleapi.Field) *ProjectsLocationsDatasetsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsLocationsDatasetsListCall) IfNoneMatch(entityTag string) *ProjectsLocationsDatasetsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsLocationsDatasetsListCall) Context(ctx context.Context) *ProjectsLocationsDatasetsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsLocationsDatasetsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsLocationsDatasetsListCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	if c.ifNoneMatch_ != "" {
		reqHeaders.Set("If-None-Match", c.ifNoneMatch_)
	}
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	c.urlParams_.Set("prettyPrint", "false")
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1p1beta1/{+parent}/datasets")
	urls += "?" + c.urlParams_.Encode()
	req, err := http.NewRequest("GET", urls, body)
	if err != nil {
		return nil, err
	}
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"parent": c.parent,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "speech.projects.locations.datasets.list" call.
// Exactly one of *ListDatasetsResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListDatasetsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsLocationsDatasetsListCall) Do(opts ...googleapi.CallOption) (*ListDatasetsResponse, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &ListDatasetsResponse{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := gensupport.DecodeResponse(target, res); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Fetch the list of dataset associated with this project.",
	//   "flatPath": "v1p1beta1/projects/{projectsId}/locations/{locationsId}/datasets",
	//   "httpMethod": "GET",
	//   "id": "speech.projects.locations.datasets.list",
	//   "parameterOrder": [
	//     "parent"
	//   ],
	//   "parameters": {
	//     "filter": {
	//       "description": "Filter the response based on display_name of the dataset. For e.g\ndisplay_name=Foo The filter string is case sensitive",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "includeModelInfo": {
	//       "description": "If true then also include information about the models built using the\ndatasets.",
	//       "location": "query",
	//       "type": "boolean"
	//     },
	//     "pageSize": {
	//       "description": "The maximum number of items to return.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "The next_page_token value returned from a previous List request, if any.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "parent": {
	//       "description": "Required. Resource name of the parent. Has the format :-\n\"projects/{project_id}/locations/{location_id}\"",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/locations/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1p1beta1/{+parent}/datasets",
	//   "response": {
	//     "$ref": "ListDatasetsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ProjectsLocationsDatasetsListCall) Pages(ctx context.Context, f func(*ListDatasetsResponse) error) error {
	c.ctx_ = ctx
	defer c.PageToken(c.urlParams_.Get("pageToken")) // reset paging to original point
	for {
		x, err := c.Do()
		if err != nil {
			return err
		}
		if err := f(x); err != nil {
			return err
		}
		if x.NextPageToken == "" {
			return nil
		}
		c.PageToken(x.NextPageToken)
	}
}

// method id "speech.projects.locations.datasets.refreshData":

type ProjectsLocationsDatasetsRefreshDataCall struct {
	s                  *Service
	name               string
	refreshdatarequest *RefreshDataRequest
	urlParams_         gensupport.URLParams
	ctx_               context.Context
	header_            http.Header
}

// RefreshData: Refresh data for a dataset.
func (r *ProjectsLocationsDatasetsService) RefreshData(name string, refreshdatarequest *RefreshDataRequest) *ProjectsLocationsDatasetsRefreshDataCall {
	c := &ProjectsLocationsDatasetsRefreshDataCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	c.refreshdatarequest = refreshdatarequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsLocationsDatasetsRefreshDataCall) Fields(s ...googleapi.Field) *ProjectsLocationsDatasetsRefreshDataCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsLocationsDatasetsRefreshDataCall) Context(ctx context.Context) *ProjectsLocationsDatasetsRefreshDataCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsLocationsDatasetsRefreshDataCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsLocationsDatasetsRefreshDataCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.refreshdatarequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	c.urlParams_.Set("prettyPrint", "false")
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1p1beta1/{+name}:refreshData")
	urls += "?" + c.urlParams_.Encode()
	req, err := http.NewRequest("POST", urls, body)
	if err != nil {
		return nil, err
	}
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "speech.projects.locations.datasets.refreshData" call.
// Exactly one of *Operation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Operation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ProjectsLocationsDatasetsRefreshDataCall) Do(opts ...googleapi.CallOption) (*Operation, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Operation{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := gensupport.DecodeResponse(target, res); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Refresh data for a dataset.",
	//   "flatPath": "v1p1beta1/projects/{projectsId}/locations/{locationsId}/datasets/{datasetsId}:refreshData",
	//   "httpMethod": "POST",
	//   "id": "speech.projects.locations.datasets.refreshData",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "The resource name of the destination dataset.",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/locations/[^/]+/datasets/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1p1beta1/{+name}:refreshData",
	//   "request": {
	//     "$ref": "RefreshDataRequest"
	//   },
	//   "response": {
	//     "$ref": "Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "speech.projects.locations.log_data_stats.list":

type ProjectsLocationsLogDataStatsListCall struct {
	s            *Service
	parent       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: List all log data stats associated with this project.
func (r *ProjectsLocationsLogDataStatsService) List(parent string) *ProjectsLocationsLogDataStatsListCall {
	c := &ProjectsLocationsLogDataStatsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.parent = parent
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsLocationsLogDataStatsListCall) Fields(s ...googleapi.Field) *ProjectsLocationsLogDataStatsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsLocationsLogDataStatsListCall) IfNoneMatch(entityTag string) *ProjectsLocationsLogDataStatsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsLocationsLogDataStatsListCall) Context(ctx context.Context) *ProjectsLocationsLogDataStatsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsLocationsLogDataStatsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsLocationsLogDataStatsListCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	if c.ifNoneMatch_ != "" {
		reqHeaders.Set("If-None-Match", c.ifNoneMatch_)
	}
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	c.urlParams_.Set("prettyPrint", "false")
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1p1beta1/{+parent}/log_data_stats")
	urls += "?" + c.urlParams_.Encode()
	req, err := http.NewRequest("GET", urls, body)
	if err != nil {
		return nil, err
	}
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"parent": c.parent,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "speech.projects.locations.log_data_stats.list" call.
// Exactly one of *ListLogDataStatsResponse or error will be non-nil.
// Any non-2xx status code is an error. Response headers are in either
// *ListLogDataStatsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsLocationsLogDataStatsListCall) Do(opts ...googleapi.CallOption) (*ListLogDataStatsResponse, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &ListLogDataStatsResponse{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := gensupport.DecodeResponse(target, res); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "List all log data stats associated with this project.",
	//   "flatPath": "v1p1beta1/projects/{projectsId}/locations/{locationsId}/log_data_stats",
	//   "httpMethod": "GET",
	//   "id": "speech.projects.locations.log_data_stats.list",
	//   "parameterOrder": [
	//     "parent"
	//   ],
	//   "parameters": {
	//     "parent": {
	//       "description": "Required. Resource name of the parent. Has the format :-\n\"projects/{project_id}/locations/{location_id}\"",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/locations/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1p1beta1/{+parent}/log_data_stats",
	//   "response": {
	//     "$ref": "ListLogDataStatsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "speech.projects.locations.models.create":

type ProjectsLocationsModelsCreateCall struct {
	s          *Service
	parent     string
	model      *Model
	urlParams_ gensupport.URLParams
	ctx_       context.Context
	header_    http.Header
}

// Create: Performs asynchronous model training for AutoML: receive
// results via the
// google.longrunning.Operations interface. Returns either
// an
// `Operation.error` or an `Operation.response` which contains a
// `Model`
// message.
func (r *ProjectsLocationsModelsService) Create(parent string, model *Model) *ProjectsLocationsModelsCreateCall {
	c := &ProjectsLocationsModelsCreateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.parent = parent
	c.model = model
	return c
}

// Name sets the optional parameter "name": Required. Resource name of
// the dataset being used to create the
// model.
// '/projects/{project_id}/locations/{location_id}/datasets/{datas
// et_id}'
func (c *ProjectsLocationsModelsCreateCall) Name(name string) *ProjectsLocationsModelsCreateCall {
	c.urlParams_.Set("name", name)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsLocationsModelsCreateCall) Fields(s ...googleapi.Field) *ProjectsLocationsModelsCreateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsLocationsModelsCreateCall) Context(ctx context.Context) *ProjectsLocationsModelsCreateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsLocationsModelsCreateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsLocationsModelsCreateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.model)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	c.urlParams_.Set("prettyPrint", "false")
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1p1beta1/{+parent}/models")
	urls += "?" + c.urlParams_.Encode()
	req, err := http.NewRequest("POST", urls, body)
	if err != nil {
		return nil, err
	}
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"parent": c.parent,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "speech.projects.locations.models.create" call.
// Exactly one of *Operation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Operation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ProjectsLocationsModelsCreateCall) Do(opts ...googleapi.CallOption) (*Operation, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Operation{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := gensupport.DecodeResponse(target, res); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Performs asynchronous model training for AutoML: receive results via the\ngoogle.longrunning.Operations interface. Returns either an\n`Operation.error` or an `Operation.response` which contains a `Model`\nmessage.",
	//   "flatPath": "v1p1beta1/projects/{projectsId}/locations/{locationsId}/models",
	//   "httpMethod": "POST",
	//   "id": "speech.projects.locations.models.create",
	//   "parameterOrder": [
	//     "parent"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "Required. Resource name of the dataset being used to create the model.\n'/projects/{project_id}/locations/{location_id}/datasets/{dataset_id}'",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "parent": {
	//       "description": "Required. Resource name of the parent. Has the format :-\n\"projects/{project_id}/locations/{location_id}\"",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/locations/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1p1beta1/{+parent}/models",
	//   "request": {
	//     "$ref": "Model"
	//   },
	//   "response": {
	//     "$ref": "Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "speech.projects.locations.models.deploy":

type ProjectsLocationsModelsDeployCall struct {
	s                  *Service
	name               string
	deploymodelrequest *DeployModelRequest
	urlParams_         gensupport.URLParams
	ctx_               context.Context
	header_            http.Header
}

// Deploy: Performs asynchronous model deployment of the model: receive
// results
// via the google.longrunning.Operations interface. After the operation
// is
// completed this returns either an `Operation.error` in case of error
// or
// a `google.protobuf.Empty` if the deployment was successful.
func (r *ProjectsLocationsModelsService) Deploy(name string, deploymodelrequest *DeployModelRequest) *ProjectsLocationsModelsDeployCall {
	c := &ProjectsLocationsModelsDeployCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	c.deploymodelrequest = deploymodelrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsLocationsModelsDeployCall) Fields(s ...googleapi.Field) *ProjectsLocationsModelsDeployCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsLocationsModelsDeployCall) Context(ctx context.Context) *ProjectsLocationsModelsDeployCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsLocationsModelsDeployCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsLocationsModelsDeployCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.deploymodelrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	c.urlParams_.Set("prettyPrint", "false")
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1p1beta1/{+name}:deploy")
	urls += "?" + c.urlParams_.Encode()
	req, err := http.NewRequest("POST", urls, body)
	if err != nil {
		return nil, err
	}
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "speech.projects.locations.models.deploy" call.
// Exactly one of *Operation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Operation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ProjectsLocationsModelsDeployCall) Do(opts ...googleapi.CallOption) (*Operation, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Operation{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := gensupport.DecodeResponse(target, res); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Performs asynchronous model deployment of the model: receive results\nvia the google.longrunning.Operations interface. After the operation is\ncompleted this returns either an `Operation.error` in case of error or\na `google.protobuf.Empty` if the deployment was successful.",
	//   "flatPath": "v1p1beta1/projects/{projectsId}/locations/{locationsId}/models/{modelsId}:deploy",
	//   "httpMethod": "POST",
	//   "id": "speech.projects.locations.models.deploy",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "Resource name of the model.\nFormat: \"projects/{project_id}/locations/{location_id}/models/{model_id}\"",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/locations/[^/]+/models/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1p1beta1/{+name}:deploy",
	//   "request": {
	//     "$ref": "DeployModelRequest"
	//   },
	//   "response": {
	//     "$ref": "Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "speech.projects.locations.models.evaluate":

type ProjectsLocationsModelsEvaluateCall struct {
	s                    *Service
	name                 string
	evaluatemodelrequest *EvaluateModelRequest
	urlParams_           gensupport.URLParams
	ctx_                 context.Context
	header_              http.Header
}

// Evaluate: Performs asynchronous evaluation of the model: receive
// results
// via the google.longrunning.Operations interface. After the operation
// is
// completed this returns either an `Operation.error` in case of error
// or
// a `EvaluateModelResponse` with the evaluation results.
func (r *ProjectsLocationsModelsService) Evaluate(name string, evaluatemodelrequest *EvaluateModelRequest) *ProjectsLocationsModelsEvaluateCall {
	c := &ProjectsLocationsModelsEvaluateCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.name = name
	c.evaluatemodelrequest = evaluatemodelrequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsLocationsModelsEvaluateCall) Fields(s ...googleapi.Field) *ProjectsLocationsModelsEvaluateCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsLocationsModelsEvaluateCall) Context(ctx context.Context) *ProjectsLocationsModelsEvaluateCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsLocationsModelsEvaluateCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsLocationsModelsEvaluateCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.evaluatemodelrequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	c.urlParams_.Set("prettyPrint", "false")
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1p1beta1/{+name}:evaluate")
	urls += "?" + c.urlParams_.Encode()
	req, err := http.NewRequest("POST", urls, body)
	if err != nil {
		return nil, err
	}
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"name": c.name,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "speech.projects.locations.models.evaluate" call.
// Exactly one of *Operation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Operation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *ProjectsLocationsModelsEvaluateCall) Do(opts ...googleapi.CallOption) (*Operation, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Operation{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := gensupport.DecodeResponse(target, res); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Performs asynchronous evaluation of the model: receive results\nvia the google.longrunning.Operations interface. After the operation is\ncompleted this returns either an `Operation.error` in case of error or\na `EvaluateModelResponse` with the evaluation results.",
	//   "flatPath": "v1p1beta1/projects/{projectsId}/locations/{locationsId}/models/{modelsId}:evaluate",
	//   "httpMethod": "POST",
	//   "id": "speech.projects.locations.models.evaluate",
	//   "parameterOrder": [
	//     "name"
	//   ],
	//   "parameters": {
	//     "name": {
	//       "description": "Resource name of the model.\nFormat: \"projects/{project_id}/locations/{location_id}/models/{model_id}\"",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/locations/[^/]+/models/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1p1beta1/{+name}:evaluate",
	//   "request": {
	//     "$ref": "EvaluateModelRequest"
	//   },
	//   "response": {
	//     "$ref": "Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "speech.projects.locations.models.list":

type ProjectsLocationsModelsListCall struct {
	s            *Service
	parent       string
	urlParams_   gensupport.URLParams
	ifNoneMatch_ string
	ctx_         context.Context
	header_      http.Header
}

// List: Fetch the list of models associated with this project.
func (r *ProjectsLocationsModelsService) List(parent string) *ProjectsLocationsModelsListCall {
	c := &ProjectsLocationsModelsListCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.parent = parent
	return c
}

// Filter sets the optional parameter "filter": Filter the response
// based on display_name of the model. For e.g
// display_name=Foo The filter string is case sensitive
func (c *ProjectsLocationsModelsListCall) Filter(filter string) *ProjectsLocationsModelsListCall {
	c.urlParams_.Set("filter", filter)
	return c
}

// PageSize sets the optional parameter "pageSize": The maximum number
// of items to return.
func (c *ProjectsLocationsModelsListCall) PageSize(pageSize int64) *ProjectsLocationsModelsListCall {
	c.urlParams_.Set("pageSize", fmt.Sprint(pageSize))
	return c
}

// PageToken sets the optional parameter "pageToken": The
// next_page_token value returned from a previous List request, if any.
func (c *ProjectsLocationsModelsListCall) PageToken(pageToken string) *ProjectsLocationsModelsListCall {
	c.urlParams_.Set("pageToken", pageToken)
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *ProjectsLocationsModelsListCall) Fields(s ...googleapi.Field) *ProjectsLocationsModelsListCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// IfNoneMatch sets the optional parameter which makes the operation
// fail if the object's ETag matches the given value. This is useful for
// getting updates only after the object has changed since the last
// request. Use googleapi.IsNotModified to check whether the response
// error from Do is the result of In-None-Match.
func (c *ProjectsLocationsModelsListCall) IfNoneMatch(entityTag string) *ProjectsLocationsModelsListCall {
	c.ifNoneMatch_ = entityTag
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *ProjectsLocationsModelsListCall) Context(ctx context.Context) *ProjectsLocationsModelsListCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *ProjectsLocationsModelsListCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *ProjectsLocationsModelsListCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	if c.ifNoneMatch_ != "" {
		reqHeaders.Set("If-None-Match", c.ifNoneMatch_)
	}
	var body io.Reader = nil
	c.urlParams_.Set("alt", alt)
	c.urlParams_.Set("prettyPrint", "false")
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1p1beta1/{+parent}/models")
	urls += "?" + c.urlParams_.Encode()
	req, err := http.NewRequest("GET", urls, body)
	if err != nil {
		return nil, err
	}
	req.Header = reqHeaders
	googleapi.Expand(req.URL, map[string]string{
		"parent": c.parent,
	})
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "speech.projects.locations.models.list" call.
// Exactly one of *ListModelsResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *ListModelsResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *ProjectsLocationsModelsListCall) Do(opts ...googleapi.CallOption) (*ListModelsResponse, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &ListModelsResponse{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := gensupport.DecodeResponse(target, res); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Fetch the list of models associated with this project.",
	//   "flatPath": "v1p1beta1/projects/{projectsId}/locations/{locationsId}/models",
	//   "httpMethod": "GET",
	//   "id": "speech.projects.locations.models.list",
	//   "parameterOrder": [
	//     "parent"
	//   ],
	//   "parameters": {
	//     "filter": {
	//       "description": "Filter the response based on display_name of the model. For e.g\ndisplay_name=Foo The filter string is case sensitive",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "pageSize": {
	//       "description": "The maximum number of items to return.",
	//       "format": "int32",
	//       "location": "query",
	//       "type": "integer"
	//     },
	//     "pageToken": {
	//       "description": "The next_page_token value returned from a previous List request, if any.",
	//       "location": "query",
	//       "type": "string"
	//     },
	//     "parent": {
	//       "description": "Required. Resource name of the parent. Has the format :-\n\"projects/{project_id}/locations/{location_id}\"",
	//       "location": "path",
	//       "pattern": "^projects/[^/]+/locations/[^/]+$",
	//       "required": true,
	//       "type": "string"
	//     }
	//   },
	//   "path": "v1p1beta1/{+parent}/models",
	//   "response": {
	//     "$ref": "ListModelsResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// Pages invokes f for each page of results.
// A non-nil error returned from f will halt the iteration.
// The provided context supersedes any context provided to the Context method.
func (c *ProjectsLocationsModelsListCall) Pages(ctx context.Context, f func(*ListModelsResponse) error) error {
	c.ctx_ = ctx
	defer c.PageToken(c.urlParams_.Get("pageToken")) // reset paging to original point
	for {
		x, err := c.Do()
		if err != nil {
			return err
		}
		if err := f(x); err != nil {
			return err
		}
		if x.NextPageToken == "" {
			return nil
		}
		c.PageToken(x.NextPageToken)
	}
}

// method id "speech.speech.longrunningrecognize":

type SpeechLongrunningrecognizeCall struct {
	s                           *Service
	longrunningrecognizerequest *LongRunningRecognizeRequest
	urlParams_                  gensupport.URLParams
	ctx_                        context.Context
	header_                     http.Header
}

// Longrunningrecognize: Performs asynchronous speech recognition:
// receive results via the
// google.longrunning.Operations interface. Returns either
// an
// `Operation.error` or an `Operation.response` which contains
// a `LongRunningRecognizeResponse` message.
func (r *SpeechService) Longrunningrecognize(longrunningrecognizerequest *LongRunningRecognizeRequest) *SpeechLongrunningrecognizeCall {
	c := &SpeechLongrunningrecognizeCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.longrunningrecognizerequest = longrunningrecognizerequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SpeechLongrunningrecognizeCall) Fields(s ...googleapi.Field) *SpeechLongrunningrecognizeCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SpeechLongrunningrecognizeCall) Context(ctx context.Context) *SpeechLongrunningrecognizeCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SpeechLongrunningrecognizeCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SpeechLongrunningrecognizeCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.longrunningrecognizerequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	c.urlParams_.Set("prettyPrint", "false")
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1p1beta1/speech:longrunningrecognize")
	urls += "?" + c.urlParams_.Encode()
	req, err := http.NewRequest("POST", urls, body)
	if err != nil {
		return nil, err
	}
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "speech.speech.longrunningrecognize" call.
// Exactly one of *Operation or error will be non-nil. Any non-2xx
// status code is an error. Response headers are in either
// *Operation.ServerResponse.Header or (if a response was returned at
// all) in error.(*googleapi.Error).Header. Use googleapi.IsNotModified
// to check whether the returned error was because
// http.StatusNotModified was returned.
func (c *SpeechLongrunningrecognizeCall) Do(opts ...googleapi.CallOption) (*Operation, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &Operation{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := gensupport.DecodeResponse(target, res); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Performs asynchronous speech recognition: receive results via the\ngoogle.longrunning.Operations interface. Returns either an\n`Operation.error` or an `Operation.response` which contains\na `LongRunningRecognizeResponse` message.",
	//   "flatPath": "v1p1beta1/speech:longrunningrecognize",
	//   "httpMethod": "POST",
	//   "id": "speech.speech.longrunningrecognize",
	//   "parameterOrder": [],
	//   "parameters": {},
	//   "path": "v1p1beta1/speech:longrunningrecognize",
	//   "request": {
	//     "$ref": "LongRunningRecognizeRequest"
	//   },
	//   "response": {
	//     "$ref": "Operation"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}

// method id "speech.speech.recognize":

type SpeechRecognizeCall struct {
	s                *Service
	recognizerequest *RecognizeRequest
	urlParams_       gensupport.URLParams
	ctx_             context.Context
	header_          http.Header
}

// Recognize: Performs synchronous speech recognition: receive results
// after all audio
// has been sent and processed.
func (r *SpeechService) Recognize(recognizerequest *RecognizeRequest) *SpeechRecognizeCall {
	c := &SpeechRecognizeCall{s: r.s, urlParams_: make(gensupport.URLParams)}
	c.recognizerequest = recognizerequest
	return c
}

// Fields allows partial responses to be retrieved. See
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
// for more information.
func (c *SpeechRecognizeCall) Fields(s ...googleapi.Field) *SpeechRecognizeCall {
	c.urlParams_.Set("fields", googleapi.CombineFields(s))
	return c
}

// Context sets the context to be used in this call's Do method. Any
// pending HTTP request will be aborted if the provided context is
// canceled.
func (c *SpeechRecognizeCall) Context(ctx context.Context) *SpeechRecognizeCall {
	c.ctx_ = ctx
	return c
}

// Header returns an http.Header that can be modified by the caller to
// add HTTP headers to the request.
func (c *SpeechRecognizeCall) Header() http.Header {
	if c.header_ == nil {
		c.header_ = make(http.Header)
	}
	return c.header_
}

func (c *SpeechRecognizeCall) doRequest(alt string) (*http.Response, error) {
	reqHeaders := make(http.Header)
	for k, v := range c.header_ {
		reqHeaders[k] = v
	}
	reqHeaders.Set("User-Agent", c.s.userAgent())
	var body io.Reader = nil
	body, err := googleapi.WithoutDataWrapper.JSONReader(c.recognizerequest)
	if err != nil {
		return nil, err
	}
	reqHeaders.Set("Content-Type", "application/json")
	c.urlParams_.Set("alt", alt)
	c.urlParams_.Set("prettyPrint", "false")
	urls := googleapi.ResolveRelative(c.s.BasePath, "v1p1beta1/speech:recognize")
	urls += "?" + c.urlParams_.Encode()
	req, err := http.NewRequest("POST", urls, body)
	if err != nil {
		return nil, err
	}
	req.Header = reqHeaders
	return gensupport.SendRequest(c.ctx_, c.s.client, req)
}

// Do executes the "speech.speech.recognize" call.
// Exactly one of *RecognizeResponse or error will be non-nil. Any
// non-2xx status code is an error. Response headers are in either
// *RecognizeResponse.ServerResponse.Header or (if a response was
// returned at all) in error.(*googleapi.Error).Header. Use
// googleapi.IsNotModified to check whether the returned error was
// because http.StatusNotModified was returned.
func (c *SpeechRecognizeCall) Do(opts ...googleapi.CallOption) (*RecognizeResponse, error) {
	gensupport.SetOptions(c.urlParams_, opts...)
	res, err := c.doRequest("json")
	if res != nil && res.StatusCode == http.StatusNotModified {
		if res.Body != nil {
			res.Body.Close()
		}
		return nil, &googleapi.Error{
			Code:   res.StatusCode,
			Header: res.Header,
		}
	}
	if err != nil {
		return nil, err
	}
	defer googleapi.CloseBody(res)
	if err := googleapi.CheckResponse(res); err != nil {
		return nil, err
	}
	ret := &RecognizeResponse{
		ServerResponse: googleapi.ServerResponse{
			Header:         res.Header,
			HTTPStatusCode: res.StatusCode,
		},
	}
	target := &ret
	if err := gensupport.DecodeResponse(target, res); err != nil {
		return nil, err
	}
	return ret, nil
	// {
	//   "description": "Performs synchronous speech recognition: receive results after all audio\nhas been sent and processed.",
	//   "flatPath": "v1p1beta1/speech:recognize",
	//   "httpMethod": "POST",
	//   "id": "speech.speech.recognize",
	//   "parameterOrder": [],
	//   "parameters": {},
	//   "path": "v1p1beta1/speech:recognize",
	//   "request": {
	//     "$ref": "RecognizeRequest"
	//   },
	//   "response": {
	//     "$ref": "RecognizeResponse"
	//   },
	//   "scopes": [
	//     "https://www.googleapis.com/auth/cloud-platform"
	//   ]
	// }

}
