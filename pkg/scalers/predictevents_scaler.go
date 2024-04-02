package scalers

import (
	"context"
	"fmt"
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

// type predictionSource struct {
// 	Type            string
// 	Endpoint        string
// 	Config          map[string]string
// 	ContainerUptime time.Duration
// 	Authentication  *authentication.AuthMeta
// }

type predictEventsScaler struct {
	metricType v2.MetricTargetType
	metadata   *predictEventsScalerMetadata
	// logger     logr.Logger
}

type predictEventsScalerMetadata struct {
	// eventSource         eventSource
	// predictionSource    predictionSource
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

	predictEventsScaler := &predictEventsScaler{}
	predictEventsScaler.metricType = metricType
	predictEventsScaler.metadata = predictEventsScalerMetadata

	return predictEventsScaler, nil
}

func (s *predictEventsScaler) GetMetricsAndActivity(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	val := 10.0

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
