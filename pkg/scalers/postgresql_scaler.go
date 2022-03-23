package scalers

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	// PostreSQL drive required for this scaler
	_ "github.com/lib/pq"
	"k8s.io/api/autoscaling/v2beta2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/metrics/pkg/apis/external_metrics"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	kedautil "github.com/kedacore/keda/v2/pkg/util"
)

type postgreSQLScaler struct {
	metadata   *postgreSQLMetadata
	connection *sql.DB
}

type postgreSQLMetadata struct {
	targetQueryValue int64
	connection       string
	query            string
	metricName       string
	scalerIndex      int
}

var postgreSQLLog = logf.Log.WithName("postgreSQL_scaler")

// NewPostgreSQLScaler creates a new postgreSQL scaler
func NewPostgreSQLScaler(config *ScalerConfig) (Scaler, error) {
	meta, err := parsePostgreSQLMetadata(config)
	if err != nil {
		return nil, fmt.Errorf("error parsing postgreSQL metadata: %s", err)
	}

	conn, err := getConnection(meta)
	if err != nil {
		return nil, fmt.Errorf("error establishing postgreSQL connection: %s", err)
	}
	return &postgreSQLScaler{
		metadata:   meta,
		connection: conn,
	}, nil
}

func parsePostgreSQLMetadata(config *ScalerConfig) (*postgreSQLMetadata, error) {
	meta := postgreSQLMetadata{}

	if val, ok := config.TriggerMetadata["query"]; ok {
		meta.query = val
	} else {
		return nil, fmt.Errorf("no query given")
	}

	if val, ok := config.TriggerMetadata["targetQueryValue"]; ok {
		targetQueryValue, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("queryValue parsing error %s", err.Error())
		}
		meta.targetQueryValue = targetQueryValue
	} else {
		return nil, fmt.Errorf("no targetQueryValue given")
	}

	switch {
	case config.AuthParams["connection"] != "":
		meta.connection = config.AuthParams["connection"]
	case config.TriggerMetadata["connectionFromEnv"] != "":
		meta.connection = config.ResolvedEnv[config.TriggerMetadata["connectionFromEnv"]]
	default:
		host, err := GetFromAuthOrMeta(config, "host")
		if err != nil {
			return nil, err
		}

		port, err := GetFromAuthOrMeta(config, "port")
		if err != nil {
			return nil, err
		}

		userName, err := GetFromAuthOrMeta(config, "userName")
		if err != nil {
			return nil, err
		}

		dbName, err := GetFromAuthOrMeta(config, "dbName")
		if err != nil {
			return nil, err
		}

		sslmode, err := GetFromAuthOrMeta(config, "sslmode")
		if err != nil {
			return nil, err
		}

		var password string
		if config.AuthParams["password"] != "" {
			password = config.AuthParams["password"]
		} else if config.TriggerMetadata["passwordFromEnv"] != "" {
			password = config.ResolvedEnv[config.TriggerMetadata["passwordFromEnv"]]
		}

		meta.connection = fmt.Sprintf(
			"host=%s port=%s user=%s dbname=%s sslmode=%s password=%s",
			host,
			port,
			userName,
			dbName,
			sslmode,
			password,
		)
	}

	if val, ok := config.TriggerMetadata["metricName"]; ok {
		meta.metricName = kedautil.NormalizeString(fmt.Sprintf("postgresql-%s", val))
	} else {
		meta.metricName = kedautil.NormalizeString("postgresql")
	}
	meta.scalerIndex = config.ScalerIndex
	return &meta, nil
}

func getConnection(meta *postgreSQLMetadata) (*sql.DB, error) {
	db, err := sql.Open("postgres", meta.connection)
	if err != nil {
		postgreSQLLog.Error(err, fmt.Sprintf("Found error opening postgreSQL: %s", err))
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		postgreSQLLog.Error(err, fmt.Sprintf("Found error pinging postgreSQL: %s", err))
		return nil, err
	}
	return db, nil
}

// Close disposes of postgres connections
func (s *postgreSQLScaler) Close(context.Context) error {
	err := s.connection.Close()
	if err != nil {
		postgreSQLLog.Error(err, "Error closing postgreSQL connection")
		return err
	}
	return nil
}

// IsActive returns true if there are pending messages to be processed
func (s *postgreSQLScaler) IsActive(ctx context.Context) (bool, error) {
	messages, err := s.getActiveNumber(ctx)
	if err != nil {
		return false, fmt.Errorf("error inspecting postgreSQL: %s", err)
	}

	return messages > 0, nil
}

func (s *postgreSQLScaler) getActiveNumber(ctx context.Context) (int64, error) {
	var id int64
	err := s.connection.QueryRowContext(ctx, s.metadata.query).Scan(&id)
	if err != nil {
		postgreSQLLog.Error(err, fmt.Sprintf("could not query postgreSQL: %s", err))
		return 0, fmt.Errorf("could not query postgreSQL: %s", err)
	}
	return id, nil
}

// GetMetricSpecForScaling returns the MetricSpec for the Horizontal Pod Autoscaler
func (s *postgreSQLScaler) GetMetricSpecForScaling(context.Context) []v2beta2.MetricSpec {
	targetQueryValue := resource.NewQuantity(s.metadata.targetQueryValue, resource.DecimalSI)

	externalMetric := &v2beta2.ExternalMetricSource{
		Metric: v2beta2.MetricIdentifier{
			Name: GenerateMetricNameWithIndex(s.metadata.scalerIndex, s.metadata.metricName),
		},
		Target: v2beta2.MetricTarget{
			Type:         v2beta2.AverageValueMetricType,
			AverageValue: targetQueryValue,
		},
	}
	metricSpec := v2beta2.MetricSpec{
		External: externalMetric, Type: externalMetricType,
	}
	return []v2beta2.MetricSpec{metricSpec}
}

// GetMetrics returns value for a supported metric and an error if there is a problem getting the metric
func (s *postgreSQLScaler) GetMetrics(ctx context.Context, metricName string, metricSelector labels.Selector) ([]external_metrics.ExternalMetricValue, error) {
	num, err := s.getActiveNumber(ctx)
	if err != nil {
		return []external_metrics.ExternalMetricValue{}, fmt.Errorf("error inspecting postgreSQL: %s", err)
	}

	metric := external_metrics.ExternalMetricValue{
		MetricName: metricName,
		Value:      *resource.NewQuantity(num, resource.DecimalSI),
		Timestamp:  metav1.Now(),
	}

	return append([]external_metrics.ExternalMetricValue{}, metric), nil
}
