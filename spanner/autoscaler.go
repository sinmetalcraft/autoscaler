package spanner

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	instanceadmin "cloud.google.com/go/spanner/admin/instance/apiv1"         // Spanner Instance Admin API client
	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb" // Spanner Instance Admin API instance protobuf definitions

	monitoringclient "cloud.google.com/go/monitoring/apiv3/v2"          // Monitoring API client
	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb" // Monitoring API protobuf definitions
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/fieldmaskpb" // For FieldMask in UpdateInstanceRequest
	"google.golang.org/protobuf/types/known/timestamppb" // For correct timestamp handling
)

var (
	// lastResized は最後にリサイズした時間です
	lastResized time.Time
)

func Handler(w http.ResponseWriter, r *http.Request) {
	project := r.URL.Query().Get("project")
	instance := r.URL.Query().Get("instance")
	puStepStr := r.URL.Query().Get("pu_step")
	puMinStr := r.URL.Query().Get("pu_min")
	puMaxStr := r.URL.Query().Get("pu_max")
	scaleUpThresholdStr := r.URL.Query().Get("scale_up_threshold")
	scaleDownThresholdStr := r.URL.Query().Get("scale_down_threshold")

	if project == "" || instance == "" || puStepStr == "" || puMinStr == "" || puMaxStr == "" {
		http.Error(w, "Missing required query parameters.", http.StatusBadRequest)
		return
	}

	puStep, err := strconv.Atoi(puStepStr)
	if err != nil {
		http.Error(w, "Invalid pu_step.", http.StatusBadRequest)
		return
	}
	puMin, err := strconv.Atoi(puMinStr)
	if err != nil {
		http.Error(w, "Invalid pu_min.", http.StatusBadRequest)
		return
	}
	puMax, err := strconv.Atoi(puMaxStr)
	if err != nil {
		http.Error(w, "Invalid pu_max.", http.StatusBadRequest)
		return
	}

	scaleUpThreshold := 50.0
	if scaleUpThresholdStr != "" {
		scaleUpThreshold, err = strconv.ParseFloat(scaleUpThresholdStr, 64)
		if err != nil {
			http.Error(w, "Invalid scale_up_threshold.", http.StatusBadRequest)
			return
		}
	}

	scaleDownThreshold := 30.0
	if scaleDownThresholdStr != "" {
		scaleDownThreshold, err = strconv.ParseFloat(scaleDownThresholdStr, 64)
		if err != nil {
			http.Error(w, "Invalid scale_down_threshold.", http.StatusBadRequest)
			return
		}
	}

	log.Printf("Request received: project=%s, instance=%s, pu_step=%d, pu_min=%d, pu_max=%d, scale_up_threshold=%.2f, scale_down_threshold=%.2f",
		project, instance, puStep, puMin, puMax, scaleUpThreshold, scaleDownThreshold)

	ctx := context.Background()
	instanceName := fmt.Sprintf("projects/%s/instances/%s", project, instance)

	// Spannerの現在のProcessing Unitを取得
	currentPU, err := getCurrentProcessingUnits(ctx, instanceName)
	if err != nil {
		log.Printf("Failed to get current processing units: %v", err)
		http.Error(w, "Failed to get current processing units.", http.StatusInternalServerError)
		return
	}
	log.Printf("Current Processing Units: %d", currentPU)

	// SpannerのCPU使用率を取得
	cpuUsage, err := getSpannerCPUUsage(ctx, project, instance)
	if err != nil {
		log.Printf("Failed to get Spanner CPU usage: %v", err)
		http.Error(w, "Failed to get Spanner CPU usage.", http.StatusInternalServerError)
		return
	}
	log.Printf("Current CPU Usage: %.2f%%", cpuUsage)

	intervalMinutesStr := os.Getenv("RESIZE_INTERVAL_MINUTES")
	if intervalMinutesStr == "" {
		intervalMinutesStr = "30" // Default interval
	}
	intervalMinutes, err := strconv.Atoi(intervalMinutesStr)
	if err != nil {
		log.Printf("Invalid RESIZE_INTERVAL_MINUTES: %v", err)
		intervalMinutes = 30
	}
	interval := time.Duration(intervalMinutes) * time.Minute

	// スケーリングロジック
	if cpuUsage > scaleUpThreshold {
		newPU := currentPU + int32(puStep)
		if newPU > int32(puMax) {
			newPU = int32(puMax)
		}
		if newPU != currentPU {
			log.Printf("Scaling up to %d PUs", newPU)
			if err := updateProcessingUnits(ctx, instanceName, newPU); err != nil {
				log.Printf("Failed to update processing units: %v", err)
				http.Error(w, "Failed to update processing units.", http.StatusInternalServerError)
				return
			}
			lastResized = time.Now()
			fmt.Fprintf(w, "Scaled up to %d PUs.", newPU)
		} else {
			fmt.Fprintf(w, "CPU usage is high, but already at max PUs.")
		}
	} else if cpuUsage < scaleDownThreshold {
		if time.Since(lastResized) < interval {
			log.Printf("Skipping scale down due to interval.")
			fmt.Fprintf(w, "Skipping scale down due to interval.")
			return
		}

		newPU := currentPU - int32(puStep)
		if newPU < int32(puMin) {
			newPU = int32(puMin)
		}
		if newPU != currentPU {
			log.Printf("Scaling down to %d PUs", newPU)
			if err := updateProcessingUnits(ctx, instanceName, newPU); err != nil {
				log.Printf("Failed to update processing units: %v", err)
				http.Error(w, "Failed to update processing units.", http.StatusInternalServerError)
				return
			}
			lastResized = time.Now()
			fmt.Fprintf(w, "Scaled down to %d PUs.", newPU)
		} else {
			fmt.Fprintf(w, "CPU usage is low, but already at min PUs.")
		}
	} else {
		log.Printf("CPU usage is within the normal range.")
		fmt.Fprintf(w, "CPU usage is within the normal range.")
	}
}

func getCurrentProcessingUnits(ctx context.Context, instanceName string) (int32, error) {
	instanceAdminClient, err := instanceadmin.NewInstanceAdminClient(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to create spanner instance admin client: %w", err)
	}
	defer instanceAdminClient.Close()

	instance, err := instanceAdminClient.GetInstance(ctx, &instancepb.GetInstanceRequest{Name: instanceName})
	if err != nil {
		return 0, fmt.Errorf("failed to get instance: %w", err)
	}

	return instance.GetProcessingUnits(), nil
}

func getSpannerCPUUsage(ctx context.Context, projectID, instanceID string) (float64, error) {
	c, err := monitoringclient.NewMetricClient(ctx)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	now := time.Now()
	startTime := now.Add(-5 * time.Minute)

	req := &monitoringpb.ListTimeSeriesRequest{
		Name:   "projects/" + projectID,
		Filter: fmt.Sprintf(`metric.type="spanner.googleapis.com/instance/cpu/utilization" resource.labels.instance_id="%s"`, instanceID),
		Interval: &monitoringpb.TimeInterval{
			StartTime: timestamppb.New(startTime),
			EndTime:   timestamppb.New(now),
		},
		View: monitoringpb.ListTimeSeriesRequest_FULL,
	}

	it := c.ListTimeSeries(ctx, req)
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("could not read time series value: %w", err)
		}
		if len(resp.GetPoints()) > 0 {
			return resp.GetPoints()[0].GetValue().GetDoubleValue() * 100, nil
		}
	}
	return 0, fmt.Errorf("no CPU usage data found for the last 5 minutes")
}

func updateProcessingUnits(ctx context.Context, instanceName string, pu int32) error {
	instanceAdminClient, err := instanceadmin.NewInstanceAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create spanner instance admin client: %w", err)
	}
	defer instanceAdminClient.Close()

	op, err := instanceAdminClient.UpdateInstance(ctx, &instancepb.UpdateInstanceRequest{
		Instance: &instancepb.Instance{
			Name:            instanceName,
			ProcessingUnits: pu,
		},
		FieldMask: &fieldmaskpb.FieldMask{
			Paths: []string{"processing_units"},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to start update instance operation: %w", err)
	}

	if _, err := op.Wait(ctx); err != nil {
		return fmt.Errorf("failed to wait for update instance operation: %w", err)
	}

	return nil
}
