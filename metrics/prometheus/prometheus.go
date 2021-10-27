package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/squareup/pranadb/conf"
	"github.com/squareup/pranadb/errors"
	"github.com/squareup/pranadb/metrics"
	"net/http"
	"sync"
)

type Factory struct {
	config     conf.Config
	lock       sync.Mutex
	httpServer *http.Server
	started    bool
}

func NewFactory(config conf.Config) metrics.Factory {
	return &Factory{config: config}
}

func (f *Factory) CreateCounter(name string, description string) (metrics.Counter, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if !f.started {
		return nil, errors.New("not started")
	}
	counter := promauto.NewCounter(prometheus.CounterOpts{
		Name: name,
		Help: description,
	})
	return &Counter{pCounter: counter}, nil
}

func (f *Factory) Start() error {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.started {
		return errors.New("already started")
	}
	metricsListenAddr := "localhost:2112"
	if f.config.MetricsHTTPListenAddr != "" {
		metricsListenAddr = f.config.MetricsHTTPListenAddr
	}
	f.httpServer = &http.Server{Addr: metricsListenAddr, Handler: promhttp.Handler()}
	f.started = true
	go func(srv *http.Server) {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("prometheus http export server failed to listen %v", err)
		} else {
			log.Debugf("Started prometheus http server on address %s", metricsListenAddr)
		}
	}(f.httpServer)
	return nil
}

func (f *Factory) Stop() error {
	f.lock.Lock()
	defer f.lock.Unlock()
	if !f.started {
		return errors.New("not started")
	}
	f.started = false
	if f.httpServer != nil {
		return f.httpServer.Close()
	}
	return nil
}

type Counter struct {
	pCounter prometheus.Counter
}

func (c *Counter) Inc() {
	c.pCounter.Inc()
}
