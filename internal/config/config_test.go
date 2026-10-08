package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadClusterAndElasticsearch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := "cluster:\n  nameEnv: CUSTOM_CLUSTER\nelasticsearch:\n  address: https://es.example:9200\n  username: writer\n  passwordEnv: CUSTOM_PASSWORD\n  index: custom-events\n  timeoutSeconds: 30\n  insecureSkipVerify: true\n"
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Cluster.NameEnv != "CUSTOM_CLUSTER" || cfg.Elasticsearch.Address != "https://es.example:9200" || cfg.Elasticsearch.Username != "writer" || cfg.Elasticsearch.PasswordEnv != "CUSTOM_PASSWORD" || cfg.Elasticsearch.Index != "custom-events" {
		t.Fatalf("unexpected config: %+v", cfg)
	}
	if cfg.Elasticsearch.TimeoutSeconds != 30 || !cfg.Elasticsearch.InsecureSkipVerify {
		t.Fatal("timeout/TLS configuration not loaded")
	}
}

func TestElasticsearchDefaults(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Elasticsearch.TimeoutSeconds != 20 || cfg.Elasticsearch.InsecureSkipVerify {
		t.Fatal("expected 20s timeout and certificate verification by default")
	}
}
