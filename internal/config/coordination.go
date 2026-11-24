package config

import (
	"crypto/sha256"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/redis/go-redis/v9"
)

var removedCoordinationEnv = []string{
	"CELLHIVE_COORDINATION_REDIS_URL", "CELLHIVE_COORDINATION_REGION",
	"CELLHIVE_SCHEDULE_MODE", "CELLHIVE_SCHEDULE_REDIS_URL", "CELLHIVE_SCHEDULE_REGION",
}

// CoordinationNamespace isolates buckets sharing one Redis database. The same
// bucket configuration must be supplied by every agent and offline initializer.
func CoordinationNamespace(bucketURL, bucketDir, endpoint string) string {
	identity := "object\x00" + strings.TrimRight(bucketURL, "/") + "\x00" + strings.TrimRight(endpoint, "/")
	if bucketURL == "" {
		absolute, err := filepath.Abs(bucketDir)
		if err != nil {
			absolute = filepath.Clean(bucketDir)
		}
		identity = "fs\x00" + absolute
	}
	sum := sha256.Sum256([]byte(identity))
	return fmt.Sprintf("bucket-%x", sum[:16])
}

func coordinationFromEnv(bucketURL, bucketDir, endpoint string) Config {
	c := Config{CoordinationBackend: "object", ScheduleMode: "cluster"}
	for _, name := range removedCoordinationEnv {
		if _, exists := os.LookupEnv(name); exists {
			c.coordinationError = fmt.Errorf("%s was removed; use CELLHIVE_COORDINATION_BACKEND=redis://host:port/db", name)
			return c
		}
	}
	raw := strings.TrimSpace(os.Getenv("CELLHIVE_COORDINATION_BACKEND"))
	switch raw {
	case "", "object", "cos":
		return c
	}
	// Never include the supplied URL or parser error in diagnostics: either can
	// contain a Redis password. Unknown schemes cannot silently select COS.
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "redis" && u.Scheme != "rediss") || u.Hostname() == "" || u.Fragment != "" {
		c.coordinationError = fmt.Errorf("CELLHIVE_COORDINATION_BACKEND requires object, cos, or a complete redis:// / rediss:// URL")
		return c
	}
	if _, err := redis.ParseURL(raw); err != nil {
		c.coordinationError = fmt.Errorf("invalid Redis URL in CELLHIVE_COORDINATION_BACKEND")
		return c
	}
	c.CoordinationBackend = "redis"
	c.CoordinationRedisURL = raw
	c.CoordinationRegion = CoordinationNamespace(bucketURL, bucketDir, endpoint)
	c.ScheduleMode = "shared"
	c.ScheduleRedisURL = raw
	c.ScheduleRegion = c.CoordinationRegion
	return c
}
