// Package config loads and validates all runtime configuration from environment variables
// (12-factor). Every knob has a safe default except secrets and connection strings.
package config

import (
	"errors"
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

type Config struct {
	Env         string `env:"APP_ENV" envDefault:"local"`
	ServiceName string `env:"SERVICE_NAME" envDefault:"go-clean-template"`
	LogLevel    string `env:"LOG_LEVEL" envDefault:"info"`

	HTTP      HTTP      `envPrefix:"HTTP_"`
	Ops       Ops       `envPrefix:"OPS_"`
	DB        DB        `envPrefix:"DB_"`
	Redis     Redis     `envPrefix:"REDIS_"`
	Kafka     Kafka     `envPrefix:"KAFKA_"`
	Outbox    Outbox    `envPrefix:"OUTBOX_"`
	RateLimit RateLimit `envPrefix:"RATE_LIMIT_"`
	Otel      Otel      `envPrefix:"OTEL_"`
}

type HTTP struct {
	Addr              string        `env:"ADDR" envDefault:":8080"`
	ReadHeaderTimeout time.Duration `env:"READ_HEADER_TIMEOUT" envDefault:"5s"`
	ReadTimeout       time.Duration `env:"READ_TIMEOUT" envDefault:"10s"`
	WriteTimeout      time.Duration `env:"WRITE_TIMEOUT" envDefault:"15s"`
	IdleTimeout       time.Duration `env:"IDLE_TIMEOUT" envDefault:"60s"`
	RequestTimeout    time.Duration `env:"REQUEST_TIMEOUT" envDefault:"10s"`
	ShutdownTimeout   time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"25s"`
	// ShutdownDelay keeps the process serving (but /readyz failing) after SIGTERM so the load
	// balancer / kube-proxy can stop routing new traffic before we close listeners.
	ShutdownDelay time.Duration `env:"SHUTDOWN_DELAY" envDefault:"3s"`
	MaxBodyBytes  int64         `env:"MAX_BODY_BYTES" envDefault:"1048576"`
	CORSOrigins   []string      `env:"CORS_ORIGINS" envSeparator:","`
	// TrustProxyHeaders honours X-Real-IP / X-Forwarded-For. Enable only behind a proxy that sets them.
	TrustProxyHeaders bool          `env:"TRUST_PROXY_HEADERS" envDefault:"false"`
	IdempotencyTTL    time.Duration `env:"IDEMPOTENCY_TTL" envDefault:"24h"`
	SSEHeartbeat      time.Duration `env:"SSE_HEARTBEAT" envDefault:"20s"`
	SSEMaxConnection  time.Duration `env:"SSE_MAX_CONNECTION" envDefault:"10m"`
}

type Ops struct {
	Addr      string `env:"ADDR" envDefault:":9090"`
	PprofAddr string `env:"PPROF_ADDR" envDefault:"127.0.0.1:6060"` // empty disables pprof
}

type DB struct {
	URL                   string        `env:"URL,required"`
	MaxConns              int32         `env:"MAX_CONNS" envDefault:"25"`
	MinConns              int32         `env:"MIN_CONNS" envDefault:"5"`
	MaxConnLifetime       time.Duration `env:"MAX_CONN_LIFETIME" envDefault:"30m"`
	MaxConnLifetimeJitter time.Duration `env:"MAX_CONN_LIFETIME_JITTER" envDefault:"5m"`
	MaxConnIdleTime       time.Duration `env:"MAX_CONN_IDLE_TIME" envDefault:"5m"`
	HealthCheckPeriod     time.Duration `env:"HEALTH_CHECK_PERIOD" envDefault:"1m"`
	ConnectTimeout        time.Duration `env:"CONNECT_TIMEOUT" envDefault:"5s"`
	StatementTimeout      time.Duration `env:"STATEMENT_TIMEOUT" envDefault:"5s"`
}

type Redis struct {
	Addr          string        `env:"ADDR" envDefault:"localhost:6379"`
	Password      string        `env:"PASSWORD"`
	DB            int           `env:"DB" envDefault:"0"`
	PoolSize      int           `env:"POOL_SIZE" envDefault:"0"` // 0 = go-redis default (10 * GOMAXPROCS)
	DialTimeout   time.Duration `env:"DIAL_TIMEOUT" envDefault:"2s"`
	ReadTimeout   time.Duration `env:"READ_TIMEOUT" envDefault:"300ms"`
	WriteTimeout  time.Duration `env:"WRITE_TIMEOUT" envDefault:"300ms"`
	OrderCacheTTL time.Duration `env:"ORDER_CACHE_TTL" envDefault:"5m"`
}

type Kafka struct {
	Brokers       []string      `env:"BROKERS" envDefault:"localhost:9092" envSeparator:","`
	ClientID      string        `env:"CLIENT_ID" envDefault:"go-clean-template"`
	Topic         string        `env:"TOPIC" envDefault:"orders.events"`
	DLQTopic      string        `env:"DLQ_TOPIC" envDefault:"orders.events.dlq"`
	ConsumerGroup string        `env:"CONSUMER_GROUP" envDefault:"order-notifier"`
	TLS           bool          `env:"TLS" envDefault:"false"`
	SASLMechanism string        `env:"SASL_MECHANISM"` // "", "SCRAM-SHA-256" or "SCRAM-SHA-512"
	SASLUser      string        `env:"SASL_USER"`
	SASLPassword  string        `env:"SASL_PASSWORD"`
	MaxAttempts   int           `env:"CONSUMER_MAX_ATTEMPTS" envDefault:"5"`
	RetryBackoff  time.Duration `env:"CONSUMER_RETRY_BACKOFF" envDefault:"200ms"`
	PollMaxRecs   int           `env:"CONSUMER_POLL_MAX_RECORDS" envDefault:"500"`
}

type Outbox struct {
	BatchSize       int           `env:"BATCH_SIZE" envDefault:"500"`
	PollInterval    time.Duration `env:"POLL_INTERVAL" envDefault:"200ms"`
	Retention       time.Duration `env:"RETENTION" envDefault:"24h"`
	CleanupInterval time.Duration `env:"CLEANUP_INTERVAL" envDefault:"10m"`
}

type RateLimit struct {
	Enabled bool    `env:"ENABLED" envDefault:"true"`
	RPS     float64 `env:"RPS" envDefault:"100"`
	Burst   int     `env:"BURST" envDefault:"200"`
}

type Otel struct {
	Endpoint    string  `env:"EXPORTER_OTLP_ENDPOINT"` // host:port (gRPC); empty disables tracing export
	Insecure    bool    `env:"EXPORTER_OTLP_INSECURE" envDefault:"true"`
	SampleRatio float64 `env:"TRACE_SAMPLE_RATIO" envDefault:"0.1"`
}

// Load parses the environment and validates the result.
func Load() (*Config, error) {
	var c Config
	if err := env.Parse(&c); err != nil {
		return nil, fmt.Errorf("parse env: %w", err)
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) IsLocal() bool { return c.Env == "local" }

func (c *Config) validate() error {
	var errs []error
	if c.DB.MaxConns < 1 || c.DB.MinConns > c.DB.MaxConns {
		errs = append(errs, errors.New("DB_MIN_CONNS must be <= DB_MAX_CONNS and DB_MAX_CONNS >= 1"))
	}
	if c.Kafka.MaxAttempts < 1 {
		errs = append(errs, errors.New("KAFKA_CONSUMER_MAX_ATTEMPTS must be >= 1"))
	}
	if c.Outbox.BatchSize < 1 {
		errs = append(errs, errors.New("OUTBOX_BATCH_SIZE must be >= 1"))
	}
	if c.RateLimit.Enabled && (c.RateLimit.RPS <= 0 || c.RateLimit.Burst < 1) {
		errs = append(errs, errors.New("RATE_LIMIT_RPS and RATE_LIMIT_BURST must be positive"))
	}
	if c.Otel.SampleRatio < 0 || c.Otel.SampleRatio > 1 {
		errs = append(errs, errors.New("OTEL_TRACE_SAMPLE_RATIO must be within [0,1]"))
	}
	if c.HTTP.WriteTimeout < c.HTTP.RequestTimeout {
		errs = append(errs, errors.New("HTTP_WRITE_TIMEOUT must be >= HTTP_REQUEST_TIMEOUT"))
	}
	return errors.Join(errs...)
}
