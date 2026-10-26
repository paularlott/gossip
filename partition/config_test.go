package partition

import (
	"testing"
	"time"
)

func TestConfigDefaults(t *testing.T) {
	cfg, err := (Config{ClusterSize: 3, Store: newMemStore()}).withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Partitions != 64 || cfg.HeartbeatInterval != 500*time.Millisecond ||
		cfg.OwnerTimeout != 3*cfg.HeartbeatInterval || cfg.Tuning.BackoffMax != cfg.OwnerTimeout ||
		cfg.Tuning.RankDelay != 2*cfg.HeartbeatInterval || cfg.Tuning.TakeoverBackoffMax != 60*time.Second ||
		cfg.Tuning.ReadyHoldDown != 2*cfg.OwnerTimeout || cfg.WriteTimeout != 5*time.Second ||
		cfg.StaleAfter != cfg.OwnerTimeout || cfg.Tuning.MaxConcurrentChanges != 256 ||
		cfg.Tuning.MaxConcurrentRequests != 1024 || cfg.Tuning.MaxConcurrentReads != 1024 ||
		cfg.Tuning.MaxChangesPerPartition != 32 || cfg.Tuning.MaxConcurrentCatchUps != 4 || cfg.Logger == nil {
		t.Errorf("unexpected defaults: %+v", cfg)
	}
	for name, bad := range map[string]Config{
		"missing cluster size": {Store: newMemStore()},
		"missing store":        {ClusterSize: 3},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := bad.withDefaults(); err == nil {
				t.Fatal("invalid configuration was accepted")
			}
		})
	}
}

func TestConfigPreservesExplicitValues(t *testing.T) {
	cfg, err := (Config{ClusterSize: 1, Store: newMemStore(),
		HeartbeatInterval: 100 * time.Millisecond, OwnerTimeout: time.Second,
		Tuning: Tuning{MaxConcurrentChanges: 7, MaxConcurrentRequests: 9, MaxConcurrentReads: 11,
			MaxChangesPerPartition: 3, MaxConcurrentCatchUps: 2},
	}).withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Tuning.BackoffMax != time.Second || cfg.Tuning.ReadyHoldDown != 2*time.Second || cfg.StaleAfter != time.Second ||
		cfg.Tuning.RankDelay != 200*time.Millisecond || cfg.Tuning.MaxConcurrentChanges != 7 ||
		cfg.Tuning.MaxConcurrentRequests != 9 || cfg.Tuning.MaxConcurrentReads != 11 ||
		cfg.Tuning.MaxChangesPerPartition != 3 || cfg.Tuning.MaxConcurrentCatchUps != 2 {
		t.Errorf("explicit values or derived defaults were changed: %+v", cfg)
	}
}
