package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadClusterAndElasticsearch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := "cluster:\n  nameEnv: CUSTOM_CLUSTER\nelasticsearch:\n  address: https://es.example:9200\n  username: writer\n  passwordEnv: CUSTOM_PASSWORD\n  index: custom-events\n"
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
}
