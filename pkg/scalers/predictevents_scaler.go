package scalers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	// "github.com/go-logr/logr"

	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
	kedautil "github.com/kedacore/keda/v2/pkg/util"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/metrics/pkg/apis/external_metrics"
)

// --------------------------------------------------------------
// -------Constants----------------------------------------------
// --------------------------------------------------------------
const (
	mlServiceHttpEndpoint       = "mlServiceHttpEndpoint"
	mlServiceHttpMethod         = "mlServiceHttpMethod"
	mlServiceHttpHeaders        = "mlServiceHttpHeaders"
	mlServiceRetrievalFrequency = "mlServiceRetrievalFrequency"

	dataInputSourceType              = "dataInputSourceType"
	dataInputSourceServerAddress     = "dataInputSourceServerAddress"
	dataInputSourceQuery             = "dataInputSourceQuery"
	dataInputSourceQueryTimeStep     = "dataInputSourceQueryTimeStep"
	dataInputSourceHistoryTimeWindow = "dataInputSourceHistoryTimeWindow"

	containerStartUpTime = "containerStartUpTime"
)

// --------------------------------------------------------------
// -------PredictEventsScaler Struct-----------------------------
// --------------------------------------------------------------

type PredictEventsScaler struct {
	metricType  v2.MetricTargetType
	metadata    *predictEventsScalerMetadata
	dataCache   dataCache
	inputSource inputSource
	mlService   mlService
}

type predictEventsScalerMetadata struct {
	activationThreshold  float64
	triggerIndex         int
	threshold            float64
	containerStartUpTime time.Duration
}

// --------------------------------------------------------------
// -------Interfaces---------------------------------------------
// --------------------------------------------------------------

type inputSource interface {
	configure(config *scalersconfig.ScalerConfig) error
	queryData() ([]dataPoint, error)
}

type mlService interface {
	getPrediction(data []byte) ([]dataPoint, error)
	getRetrievalFrequency() time.Duration
}

type dataCache interface {
	getData(time time.Time) (float64, error)
	setData(data []dataPoint) error
}

// --------------------------------------------------------------
// -------Structs------------------------------------------------
// --------------------------------------------------------------

type inputSourcePrometheus struct {
	serverAddress      string
	query              string
	historyTimeWindow  time.Duration
	timeStep           time.Duration
	dataInputFrequency time.Duration
}

type mlServiceHttp struct {
	endpoint                     string
	method                       string
	headers                      map[string]string
	predictionRetrievalFrequency time.Duration
}

type dataCacheInMemory struct {
	sync.RWMutex
	data map[time.Time]float64
}

type dataPoint struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

type predictions struct {
	Data []dataPoint `json:"predictions"`
}

type inputData struct {
	Data []dataPoint `json:"data"`
}

// --------------------------------------------------------------
// -------Input Source Implementation----------------------------
// --------------------------------------------------------------
func (s *inputSourcePrometheus) configure(config *scalersconfig.ScalerConfig) error {

	if val, ok := config.TriggerMetadata[dataInputSourceServerAddress]; ok && val != "" {
		s.serverAddress = val
	} else {
		return fmt.Errorf("no %s given", dataInputSourceServerAddress)
	}

	if val, ok := config.TriggerMetadata[dataInputSourceQuery]; ok && val != "" {
		s.query = val
	} else {
		return fmt.Errorf("no %s given", dataInputSourceQuery)
	}

	if val, ok := config.TriggerMetadata[dataInputSourceHistoryTimeWindow]; ok && val != "" {
		parsedVal, err := time.ParseDuration(val)
		if err != nil {
			return fmt.Errorf("error parsing %s: %w", dataInputSourceHistoryTimeWindow, err)
		}
		s.historyTimeWindow = parsedVal
	} else {
		return fmt.Errorf("no %s given", dataInputSourceHistoryTimeWindow)
	}

	if val, ok := config.TriggerMetadata[dataInputSourceQueryTimeStep]; ok && val != "" {
		parsedVal, err := time.ParseDuration(val)
		if err != nil {
			return fmt.Errorf("error parsing %s: %w", dataInputSourceQueryTimeStep, err)
		}
		s.timeStep = parsedVal
	} else {
		return fmt.Errorf("no %s given", dataInputSourceQueryTimeStep)
	}

	return nil
}

func (s *inputSourcePrometheus) queryData() ([]dataPoint, error) {
	data, err := queryPrometheus(s.serverAddress, s.query, s.historyTimeWindow)
	if err != nil {
		return nil, fmt.Errorf("error querying Prometheus: %s", err)
	}
	return data, nil
}

// --------------------------------------------------------------
// -------ML Service Implementation------------------------------
// --------------------------------------------------------------
func (s *mlServiceHttp) getPrediction(data []byte) ([]dataPoint, error) {
	body, _, err := callHttp(s.endpoint, s.method, s.headers, data)
	if err != nil {
		return nil, fmt.Errorf("error calling prediction source: %s", err)
	}
	var result predictions
	err = json.Unmarshal(body, &result)
	if err != nil {
		// handle error
		fmt.Println("Error: ", err)
		return nil, nil
	}
	return result.Data, nil
}

func (s *mlServiceHttp) getRetrievalFrequency() time.Duration {
	return s.predictionRetrievalFrequency
}

// --------------------------------------------------------------
// -------Data Cache Implementation------------------------------
// --------------------------------------------------------------
func (s *dataCacheInMemory) getData(time time.Time) (float64, error) {
	s.RLock()
	defer s.RUnlock()
	var nearest *dataPoint
	for timestamp, value := range s.data {
		if timestamp.After(time) {
			if nearest == nil || timestamp.Before(nearest.Timestamp) {
				nearest = &dataPoint{
					Timestamp: timestamp,
					Value:     value,
				}
			}
		}
	}
	return nearest.Value, nil
}

func (s *dataCacheInMemory) setData(dataSet []dataPoint) error {
	s.Lock()
	defer s.Unlock()
	for _, data := range dataSet {
		s.data[data.Timestamp] = data.Value
	}
	return nil
}

// --------------------------------------------------------------
// -------Predict Events Scaler utility functions----------------
// --------------------------------------------------------------
func setMetadata(config *scalersconfig.ScalerConfig) (*predictEventsScalerMetadata, error) {

	meta := &predictEventsScalerMetadata{}

	if val, ok := config.TriggerMetadata["threshold"]; ok && val != "" {
		parsedVal, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing %s: %w", "threshold", err)
		}
		meta.threshold = parsedVal
	} else {
		return nil, fmt.Errorf("no %s given", "threshold")
	}

	if val, ok := config.TriggerMetadata["activationThreshold"]; ok && val != "" {
		parsedVal, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing %s: %w", "activationThreshold", err)
		}
		meta.activationThreshold = parsedVal
	} else {
		return nil, fmt.Errorf("no %s given", "activationThreshold")
	}

	if val, ok := config.TriggerMetadata[containerStartUpTime]; ok && val != "" {
		parsedVal, err := time.ParseDuration(val)
		if err != nil {
			return nil, fmt.Errorf("error parsing %s: %w", containerStartUpTime, err)
		}
		meta.containerStartUpTime = parsedVal
	} else {
		return nil, fmt.Errorf("no %s given", containerStartUpTime)
	}

	meta.triggerIndex = config.TriggerIndex
	return meta, nil
}

func setInputSource(config *scalersconfig.ScalerConfig) (inputSource, error) {
	if val, ok := config.TriggerMetadata[dataInputSourceType]; ok && val != "" {
		if val == "prometheus" {
			inputSource := &inputSourcePrometheus{}
			err := inputSource.configure(config)
			if err != nil {
				return nil, fmt.Errorf("error configuring input source: %s", err)
			}
			return inputSource, nil
		}
	}
	return nil, fmt.Errorf("invalid input source type given")
}

func setMLService(config *scalersconfig.ScalerConfig) (mlService, error) {

	mlService := &mlServiceHttp{}

	if val, ok := config.TriggerMetadata[mlServiceHttpEndpoint]; ok && val != "" {
		mlService.endpoint = val
	} else {
		return nil, fmt.Errorf("no %s given", mlServiceHttpEndpoint)
	}

	if val, ok := config.TriggerMetadata[mlServiceHttpMethod]; ok && val != "" {
		mlService.method = val
	} else {
		return nil, fmt.Errorf("no %s given", mlServiceHttpMethod)
	}

	if val, ok := config.TriggerMetadata[mlServiceHttpHeaders]; ok && val != "" {
		mlService.headers = getMapFromStr(val)
	} else {
		return nil, fmt.Errorf("no %s given", mlServiceHttpHeaders)
	}

	if val, ok := config.TriggerMetadata[mlServiceRetrievalFrequency]; ok && val != "" {
		parsedVal, err := time.ParseDuration(val)
		if err != nil {
			return nil, fmt.Errorf("error parsing %s: %w", mlServiceRetrievalFrequency, err)
		}
		mlService.predictionRetrievalFrequency = parsedVal
	} else {
		return nil, fmt.Errorf("no %s given", mlServiceRetrievalFrequency)
	}

	return mlService, nil
}

func setDataCache(config *scalersconfig.ScalerConfig) (dataCache, error) {
	return &dataCacheInMemory{}, nil
}

// --------------------------------------------------------------
// -------Predict Events Scaler Implementation------------------
// --------------------------------------------------------------
func (s *PredictEventsScaler) configure(ctx context.Context, config *scalersconfig.ScalerConfig) error {

	metricType, err := GetMetricTargetType(config)
	if err != nil {
		return fmt.Errorf("error getting scaler metric type: %w", err)
	}

	metadata, err := setMetadata(config)
	if err != nil {
		return fmt.Errorf("error setting medatdat: %w", err)
	}

	inputSource, err := setInputSource(config)
	if err != nil {
		return fmt.Errorf("error setting input source: %w", err)
	}

	mlService, err := setMLService(config)
	if err != nil {
		return fmt.Errorf("error setting ml service: %w", err)
	}

	dataCache, err := setDataCache(config)
	if err != nil {
		return fmt.Errorf("error setting data cache: %w", err)
	}

	s.metricType = metricType
	s.metadata = metadata
	s.inputSource = inputSource
	s.mlService = mlService
	s.dataCache = dataCache

	return nil
}

func (s *PredictEventsScaler) run() error {
	// Fetch data from input source
	queryData, err := s.inputSource.queryData()
	if err != nil {
		return fmt.Errorf("error querying data: %w", err)
	}

	// Send data to ML service
	inputData := inputData{Data: queryData}
	inputJson, err := json.Marshal(inputData)
	if err != nil {
		return fmt.Errorf("error marshalling input data: %w", err)
	}

	predictions, err := s.mlService.getPrediction(inputJson)
	if err != nil {
		return fmt.Errorf("error getting prediction: %w", err)
	}

	// Set data in cache
	err = s.dataCache.setData(predictions)
	if err != nil {
		return fmt.Errorf("error setting data in cache: %w", err)
	}

	return nil
}

func (s *PredictEventsScaler) initialize(ctx context.Context) error {

	err := s.run()
	if err != nil {
		return err
	}

	predictionRetrievalFreq := s.mlService.(*mlServiceHttp).predictionRetrievalFrequency
	predictionRetrievalTicker := time.NewTicker(predictionRetrievalFreq)
	predictionDone := make(chan bool)

	go func() {
		for {
			select {
			case <-predictionDone:
				return
			case t := <-predictionRetrievalTicker.C:
				fmt.Println("Tick at", t)
				err := s.run()
				if err != nil {
					// handle error
				}
			}
		}
	}()

	return nil
}

func (s *PredictEventsScaler) GetMetricSpecForScaling(context.Context) []v2.MetricSpec {
	metricName := kedautil.NormalizeString("predict-events")
	externalMetric := &v2.ExternalMetricSource{
		Metric: v2.MetricIdentifier{
			Name: GenerateMetricNameWithIndex(s.metadata.triggerIndex, metricName),
		},
		Target: GetMetricTargetMili(s.metricType, s.metadata.threshold),
	}
	metricSpec := v2.MetricSpec{
		External: externalMetric, Type: externalMetricType,
	}
	return []v2.MetricSpec{metricSpec}
}

func (s *PredictEventsScaler) GetMetricsAndActivity(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {

	val, err := s.dataCache.getData(time.Now())
	if err != nil {
		return nil, false, fmt.Errorf("error while getting data from cache: %s", err)
	}

	metric := GenerateMetricInMili(metricName, val)

	return []external_metrics.ExternalMetricValue{metric}, val > s.metadata.activationThreshold, nil
}

func (s *PredictEventsScaler) Close(context.Context) error {
	return nil
}

// NewPredictEventsScaler creates a new instance of the PredictEventsScaler

func NewPredictEventsScaler(ctx context.Context, config *scalersconfig.ScalerConfig) (*PredictEventsScaler, error) {

	predictEventsScaler := &PredictEventsScaler{}

	err := predictEventsScaler.configure(ctx, config)
	if err != nil {
		return nil, err
	}

	err = predictEventsScaler.initialize(ctx)
	if err != nil {
		return nil, err
	}

	return predictEventsScaler, nil
}

func getMapFromStr(str string) map[string]string {
	m := make(map[string]string)
	for _, s := range strings.Split(str, "\n") {
		kv := strings.Split(s, ":")
		m[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
	}
	return m
}

func callAPI(url string, method string, headers map[string]string, data []byte) (map[string]interface{}, error) {
	// Convert the data to JSON
	// jsonData, err := json.Marshal(data)
	// if err != nil {
	// 	return nil, err
	// }

	// Create a new request
	req, err := http.NewRequest(method, url, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		if k == "Content-Type" {
			continue
		}
		req.Header.Set(k, v)
	}

	// Send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Println("Error closing body: ", err)
		}
	}(resp.Body)

	// Read the response
	body, _ := ioutil.ReadAll(resp.Body)

	// Decode the JSON response
	var result map[string]interface{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func callHttp(url string, method string, headers map[string]string, data []byte) ([]byte, int, error) {

	// Create a new request
	req, err := http.NewRequest(method, url, bytes.NewBuffer(data))
	if err != nil {
		return nil, 0, err
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		if k == "Content-Type" {
			continue
		}
		req.Header.Set(k, v)
	}

	// Send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Println("Error closing body: ", err)
		}
	}(resp.Body)

	// Read the response
	body, _ := ioutil.ReadAll(resp.Body)

	return body, resp.StatusCode, nil
}

func queryPrometheus(serverURL, query string, timeWindowDuration time.Duration) ([]dataPoint, error) {
	// Create a new Prometheus API client
	client, err := api.NewClient(api.Config{
		Address: serverURL,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating Prometheus client: %w", err)
	}

	// Create a new Prometheus v1 API interface
	promApi := v1.NewAPI(client)

	//timeWindowDuration, err := time.ParseDuration(timeWindow)
	//if err != nil {
	//	return nil, fmt.Errorf("error parsing time window: %w", err)
	//}

	// Query Prometheus
	result, warnings, err := promApi.QueryRange(context.Background(), query, v1.Range{
		Start: time.Now().Add(-timeWindowDuration),
		End:   time.Now(),
		Step:  time.Minute,
	})

	if len(warnings) > 0 {
		fmt.Printf("Warnings: %v\n", warnings)
	}

	if err != nil {
		return nil, fmt.Errorf("error querying Prometheus: %w", err)
	}

	if result == nil {
		return nil, fmt.Errorf("promethues result is nil")

	} else {
		fmt.Println("Result type: ", result.Type())
	}

	// Return the result as a string
	parsedResult, err := parsePrometheusResult(result)
	if err != nil {
		return nil, fmt.Errorf("error parsing Prometheus result: %w", err)
	}
	return parsedResult, nil
}

func parsePrometheusResult(result model.Value) ([]dataPoint, error) {
	var out []dataPoint
	fmt.Println("Result type: ", result.Type())
	switch result.Type() {
	case model.ValVector:
		if res, ok := result.(model.Vector); ok {
			for _, val := range res {
				t := val.Timestamp.Time()
				v := float64(val.Value)
				out = append(out, dataPoint{t, v})
			}
		}
	case model.ValMatrix:
		if res, ok := result.(model.Matrix); ok {
			for _, val := range res {
				for _, v := range val.Values {
					t := v.Timestamp.Time()
					v := float64(v.Value)
					out = append(out, dataPoint{t, v})
				}
			}
		}
	case model.ValScalar:
		if res, ok := result.(*model.Scalar); ok {
			t := res.Timestamp.Time()
			v := float64(res.Value)
			out = append(out, dataPoint{t, v})
		}
	case model.ValString:
		if res, ok := result.(*model.String); ok {
			t := res.Timestamp.Time()

			s, err := strconv.ParseFloat(res.Value, 64)
			if err != nil {
				return nil, err
			}
			out = append(out, dataPoint{t, s})
		}
	default:
		return nil, errors.ErrUnsupported
	}
	return out, nil
}
