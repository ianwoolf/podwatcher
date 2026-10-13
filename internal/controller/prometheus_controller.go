package controller

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"podwatcher/internal/prometheus"

	"github.com/gin-gonic/gin"
)

// PrometheusClient is the subset of prometheus.Client exposed to the controller.
type PrometheusClient interface {
	QueryRange(ctx context.Context, query string, start, end time.Time, step time.Duration) ([]prometheus.Result, error)
}

// PrometheusGateway proxies Prometheus HTTP API requests, parsing Prometheus
// query string parameters and forwarding them to the underlying client.
type PrometheusGateway struct {
	client PrometheusClient
}

func NewPrometheusGateway(client PrometheusClient) *PrometheusGateway {
	return &PrometheusGateway{client: client}
}

// QueryRange handles GET /api/v1/prometheus/query_range.
// Query parameters follow the canonical Prometheus /api/v1/query_range form:
//
//	query=<PromQL>
//	start=<RFC3339 or Unix seconds>
//	end=<RFC3339 or Unix seconds>
//	step=<Go duration or seconds, e.g. 30s, 1m, 60>
func (g *PrometheusGateway) QueryRange(ctx *gin.Context) {
	query := ctx.Query("query")
	if query == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "query is required"})
		return
	}

	start, err := parseTime(ctx.Query("start"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid start: " + err.Error()})
		return
	}
	end, err := parseTime(ctx.Query("end"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid end: " + err.Error()})
		return
	}
	if end.Before(start) {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "end must be >= start"})
		return
	}

	step, err := parseStep(ctx.Query("step"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid step: " + err.Error()})
		return
	}
	if step <= 0 {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "step must be positive"})
		return
	}

	results, err := g.client.QueryRange(ctx.Request.Context(), query, start, end, step)
	if err != nil {
		ctx.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"status": "success",
		"data": gin.H{
			"resultType": "matrix",
			"result":     results,
		},
	})
}

// parseTime accepts RFC3339 or a fractional Unix timestamp (seconds).
func parseTime(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, errors.New("required")
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t.UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t.UTC(), nil
	}
	// Fractional Unix seconds.
	var sec float64
	if _, err := strconv.ParseFloat(raw, 64); err == nil {
		sec, _ = strconv.ParseFloat(raw, 64)
		return time.Unix(0, int64(sec*1e9)).UTC(), nil
	}
	return time.Time{}, errors.New("must be RFC3339 or Unix seconds")
}

// parseStep accepts Go duration strings ("30s", "1m", "5m30s") or a bare
// number interpreted as seconds.
func parseStep(raw string) (time.Duration, error) {
	if raw == "" {
		return 0, errors.New("required")
	}
	if d, err := time.ParseDuration(raw); err == nil {
		return d, nil
	}
	var sec float64
	if _, err := strconv.ParseFloat(raw, 64); err == nil {
		sec, _ = strconv.ParseFloat(raw, 64)
		return time.Duration(sec * float64(time.Second)), nil
	}
	return 0, errors.New("must be Go duration (e.g. 30s) or seconds")
}
