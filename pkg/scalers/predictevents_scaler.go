package scalers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
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

type dataPoint struct {
	Timestamp time.Time
	Value     float64
}

type eventSource struct {
	sourceType string
	metadata   *eventSourcePrometheusMetadata
	// Authentication *authentication.AuthMeta
}

type eventSourcePrometheusMetadata struct {
	ServerAddress     string
	Query             string
	HistoryTimeWindow string
}

type predictionSource struct {
	HttpEndpoint string
	HttpMethod   string
	HttpHeaders  map[string]string
	eventSource  eventSource
	// ContainerUptimeSeconds int64
	// Authentication  *authentication.AuthMeta
}

type predictEventsScaler struct {
	metricType v2.MetricTargetType
	metadata   *predictEventsScalerMetadata
	// logger     logr.Logger
}

type predictEventsScalerMetadata struct {
	predictionSource    predictionSource
	activationThreshold float64 // Add the missing activationThreshold field
	triggerIndex        int
	threshold           float64
}

func (s *eventSource) QueryData() ([]byte, error) {
	switch s.sourceType {
	case "prometheus":
		result, err := QueryPrometheus(s.metadata.ServerAddress, s.metadata.Query, s.metadata.HistoryTimeWindow)
		if err != nil {
			return nil, err
		}
		jsonResult, err := json.Marshal(result)
		if err != nil {
			return nil, err
		}
		return jsonResult, nil
	default:
		return nil, errors.New("unsupported event source type")
	}
}

func getMapFromStr(str string) map[string]string {
	m := make(map[string]string)
	for _, s := range strings.Split(str, "\n") {
		kv := strings.Split(s, ":")
		m[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
	}
	return m
}

func setPredictEventsScalerMetadata(meta *predictEventsScalerMetadata, config *scalersconfig.ScalerConfig) error {

	if val, ok := config.TriggerMetadata["threshold"]; ok && val != "" {
		parsedVal, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return fmt.Errorf("error parsing %s: %w", "threshold", err)
		}
		meta.threshold = parsedVal
	} else {
		return fmt.Errorf("no %s given", "threshold")
	}

	if val, ok := config.TriggerMetadata["activationThreshold"]; ok && val != "" {
		meta.activationThreshold, _ = strconv.ParseFloat(val, 64)
	} else {
		return fmt.Errorf("no %s given", "activationThreshold")
	}

	if val, ok := config.TriggerMetadata["predictionSourceHttpEndpoint"]; ok && val != "" {
		meta.predictionSource.HttpEndpoint = val
	} else {
		return fmt.Errorf("no %s given", "predictionSourceHttpEndpoint")
	}

	if val, ok := config.TriggerMetadata["predictionSourceHttpMethod"]; ok && val != "" {
		meta.predictionSource.HttpMethod = val
	} else {
		return fmt.Errorf("no %s given", "predictionSourceHttpMethod")
	}

	if val, ok := config.TriggerMetadata["predictionSourceHttpHeaders"]; ok && val != "" {
		meta.predictionSource.HttpHeaders = getMapFromStr(val)
	} else {
		return fmt.Errorf("no %s given", "predictionSourceHttpHeaders")
	}

	if val, ok := config.TriggerMetadata["eventSourceType"]; ok && val != "" {
		meta.predictionSource.eventSource.sourceType = val
	} else {
		return fmt.Errorf("no %s given", "eventSourceType")

	}

	if meta.predictionSource.eventSource.sourceType == "prometheus" {
		meta.predictionSource.eventSource.metadata = &eventSourcePrometheusMetadata{}
		if val, ok := config.TriggerMetadata["prometheusServerAddress"]; ok && val != "" {
			meta.predictionSource.eventSource.metadata.ServerAddress = val
		} else {
			return fmt.Errorf("no %s given", "prometheusServerAddress")
		}

		if val, ok := config.TriggerMetadata["prometheusQuery"]; ok && val != "" {
			meta.predictionSource.eventSource.metadata.Query = val
		} else {
			return fmt.Errorf("no %s given", "prometheusQuery")
		}

		if val, ok := config.TriggerMetadata["prometheusQueryHistoryTimeWindow"]; ok && val != "" {
			meta.predictionSource.eventSource.metadata.HistoryTimeWindow = val
		} else {
			return fmt.Errorf("no %s given", "prometheusQueryHistoryTimeWindow")
		}

	}

	return nil
}

func NewPredictEventsScaler(ctx context.Context, config *scalersconfig.ScalerConfig) (*predictEventsScaler, error) {

	// eventSource := &eventSource{}

	predictEventsScalerMetadata := &predictEventsScalerMetadata{}
	predictEventsScalerMetadata.triggerIndex = config.TriggerIndex

	metricType, err := GetMetricTargetType(config)
	if err != nil {
		return nil, fmt.Errorf("error getting scaler metric type: %w", err)
	}

	err = setPredictEventsScalerMetadata(predictEventsScalerMetadata, config)
	if err != nil {
		return nil, err
	}

	predictEventsScaler := &predictEventsScaler{}
	predictEventsScaler.metricType = metricType
	predictEventsScaler.metadata = predictEventsScalerMetadata

	return predictEventsScaler, nil
}

func (s *predictEventsScaler) GetMetricsAndActivity(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	val, err := s.metadata.predictionSource.getPrediction()

	if err != nil {
		return nil, false, err
	}

	metric := GenerateMetricInMili(metricName, val)

	return []external_metrics.ExternalMetricValue{metric}, val > s.metadata.activationThreshold, nil
}

func (s *predictEventsScaler) GetMetricSpecForScaling(context.Context) []v2.MetricSpec {
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

func (s *predictEventsScaler) Close(context.Context) error {
	return nil
}

func (s *predictEventsScaler) IsActive(ctx context.Context) (bool, error) {
	// implement the IsActive method
	return true, nil
}

func (s *predictionSource) getPrediction() (float64, error) {

	// Query Prometheus
	// result, err := QueryPrometheus(s.eventSource, s.HttpMethod, s.HttpHeaders, s.HttpData)

	data, err := s.eventSource.QueryData()
	if err != nil {
		return 0, fmt.Errorf("error querying event source: %s", err)
	}
	res, err := callAPI(s.HttpEndpoint, s.HttpMethod, s.HttpHeaders, data)
	if err != nil {
		err := fmt.Errorf("error calling prediction source: %s", err)
		return 0, err
	}
	return res["prediction"].(float64), nil

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
	defer resp.Body.Close()

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

func QueryPrometheus(serverURL, query string, timeWindow string) ([]dataPoint, error) {
	// Create a new Prometheus API client
	client, err := api.NewClient(api.Config{
		Address: serverURL,
	})
	if err != nil {
		return nil, fmt.Errorf("error creating Prometheus client: %w", err)
	}

	// Create a new Prometheus v1 API interface
	api := v1.NewAPI(client)

	timeWindowDuration, err := time.ParseDuration(timeWindow)
	if err != nil {
		return nil, fmt.Errorf("error parsing time window: %w", err)
	}

	// Query Prometheus
	result, warnings, err := api.QueryRange(context.Background(), query, v1.Range{
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
