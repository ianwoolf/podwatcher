package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"podwatcher/internal/controller"
	"podwatcher/internal/handler"
	"podwatcher/internal/informer"
	"podwatcher/internal/service"
	"podwatcher/pkg/k8s"

	"github.com/gin-gonic/gin"
	"k8s.io/klog/v2"
)

var (
	kubeconfig = flag.String("kubeconfig", "", "Path to kubeconfig file")
	port       = flag.String("port", "8080", "HTTP server port")
)

func main() {
	flag.Parse()

	klog.InitFlags(nil)
	defer klog.Flush()

	k8sClient, err := k8s.NewClient(*kubeconfig)
	if err != nil {
		klog.Fatalf("Failed to create k8s client: %v", err)
	}
	klog.Info("Kubernetes client created successfully")

	eventHandler := handler.NewPodEventHandler()

	podInformerManager := informer.NewPodInformerManager(k8sClient, eventHandler)

	if err := podInformerManager.Start(); err != nil {
		klog.Fatalf("Failed to start informer: %v", err)
	}

	podService := service.NewPodService(k8sClient, eventHandler)
	podController := controller.NewPodController(podService)

	router := setupRouter(podController)

	srv := &http.Server{
		Addr:    ":" + *port,
		Handler: router,
	}

	go func() {
		klog.Infof("Starting HTTP server on port %s", *port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			klog.Fatalf("Failed to start server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	klog.Info("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	podInformerManager.Stop()

	if err := srv.Shutdown(ctx); err != nil {
		klog.Errorf("Server forced to shutdown: %v", err)
	}

	klog.Info("Server exited")
}

func setupRouter(podController *controller.PodController) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)

	router := gin.Default()

	router.Use(gin.Recovery())
	router.Use(gin.Logger())

	api := router.Group("/api/v1")
	{
		api.GET("/health", podController.HealthCheck)

		api.GET("/pods", podController.ListPods)
		api.GET("/pods/:namespace/:name", podController.GetPod)

		api.GET("/events", podController.GetEvents)

		api.GET("/stats/pods", podController.GetPodStats)
	}

	router.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "Gin K8s Informer Server",
			"version": "1.0.0",
			"endpoints": []string{
				"GET  /api/v1/health",
				"GET  /api/v1/pods?namespace=&labelSelector=",
				"GET  /api/v1/pods/:namespace/:name",
				"GET  /api/v1/events?limit=",
				"GET  /api/v1/stats/pods?namespace=",
			},
		})
	})

	return router
}
