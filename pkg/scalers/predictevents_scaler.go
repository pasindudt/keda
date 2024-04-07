package scalers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	// "github.com/go-logr/logr"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
	kedautil "github.com/kedacore/keda/v2/pkg/util"
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/metrics/pkg/apis/external_metrics"
)

// type eventSource struct {
// 	Type           string
// 	Authentication *authentication.AuthMeta
// }

type predictionSource struct {
	Endpoint               string
	ContainerUptimeSeconds int64
	// Authentication  *authentication.AuthMeta
}

type predictEventsScaler struct {
	metricType v2.MetricTargetType
	metadata   *predictEventsScalerMetadata
	// logger     logr.Logger
}

type predictEventsScalerMetadata struct {
	// eventSource         eventSource
	predictionSource    predictionSource
	activationThreshold float64 // Add the missing activationThreshold field
	triggerIndex        int
	threshold           float64
}

func NewPredictEventsScaler(ctx context.Context, config *scalersconfig.ScalerConfig) (*predictEventsScaler, error) {

	predictEventsScalerMetadata := &predictEventsScalerMetadata{}
	predictEventsScalerMetadata.triggerIndex = config.TriggerIndex

	metricType, err := GetMetricTargetType(config)
	if err != nil {
		return nil, fmt.Errorf("error getting scaler metric type: %w", err)
	}

	if val, ok := config.TriggerMetadata["threshold"]; ok && val != "" {
		predictEventsScalerMetadata.threshold, _ = strconv.ParseFloat(val, 64)
	} else {
		return nil, fmt.Errorf("no %s given", "threshold")
	}

	if val, ok := config.TriggerMetadata["activationThreshold"]; ok && val != "" {
		predictEventsScalerMetadata.activationThreshold, _ = strconv.ParseFloat(val, 64)
	} else {
		return nil, fmt.Errorf("no %s given", "activationThreshold")
	}

	if val, ok := config.TriggerMetadata["predictionSourceEndpoint"]; ok && val != "" {
		predictEventsScalerMetadata.predictionSource.Endpoint = val
	} else {
		return nil, fmt.Errorf("no %s given", "predictionSourceEndpoint")
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

	data := map[string]interface{}{"uptime": s.ContainerUptimeSeconds}
	res, err := callAPI(s.Endpoint, data)
	if err != nil {
		err := fmt.Errorf("Error calling prediction source: %s", err)
		return 0, err
	}
	return res["prediction"].(float64), nil

}

func callAPI(url string, data map[string]interface{}) (map[string]interface{}, error) {
	// Convert the data to JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	// Create a new request
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

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
