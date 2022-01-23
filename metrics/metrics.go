package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/conf"
)

type (
	Labels        = prometheus.Labels
	Counter       = prometheus.Counter
	CounterVec    = prometheus.CounterVec
	CounterOpts   = prometheus.CounterOpts
	Gauge         = prometheus.Gauge
	GaugeVec      = prometheus.GaugeVec
	GaugeOpts     = prometheus.GaugeOpts
	SummaryOpts   = prometheus.SummaryOpts
	SummaryVec    = prometheus.SummaryVec
	HistogramOpts = prometheus.HistogramOpts
	HistogramVec  = prometheus.HistogramVec
	Observer      = prometheus.Observer
)

type Server struct {
	config     conf.Config
	httpServer *http.Server
}

func NewServer(config conf.Config) *Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	return &Server{
		config: config,
		httpServer: &http.Server{
			Addr:    config.MetricsBind,
			Handler: mux,
		},
	}
}

func (s *Server) Start() error {
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("prometheus http export server failed to listen %v", err)
		} else {
			log.Debugf("Started prometheus http server on address %s", s.config.MetricsBind)
		}
	}()
	return nil
}

func (s *Server) Stop() error {
	if s.httpServer != nil {
		return s.httpServer.Close()
	}
	return nil
}
