package worker

import (
	"context"
	"testing"
)

func TestMasterMethodsRejectUninitializedMaster(t *testing.T) {
	for name, ms := range map[string]*Master{"zero": {}, "nil": nil} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if got := ms.Schedule(phaseErrorTask{}); got != ErrMasterNotInitialized {
				t.Fatalf("Schedule() error = %v", got)
			}
			if _, got := ms.Submit(ctx, phaseErrorTask{}); got != ErrMasterNotInitialized {
				t.Fatalf("Submit() error = %v", got)
			}
			if _, got := ms.TrySubmit(phaseErrorTask{}); got != ErrMasterNotInitialized {
				t.Fatalf("TrySubmit() error = %v", got)
			}
			if got := ms.Shutdown(ctx); got != ErrMasterNotInitialized {
				t.Fatalf("Shutdown() error = %v", got)
			}
			if got := ms.Wait(ctx); got != ErrMasterNotInitialized {
				t.Fatalf("Wait() error = %v", got)
			}
			assertNotPanics(t, ms.Stop)
			if got := ms.Workers(); got != 0 {
				t.Fatalf("Workers() = %d, want 0", got)
			}
			if got := ms.Stats(); got != (MasterStats{}) {
				t.Fatalf("Stats() = %+v", got)
			}
		})
	}
}
