package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/redis/agent-filesystem/mount/client"
)

func seedRecoveryReadOnlyDirectories(t *testing.T, env *syncTestEnv, existing bool) {
	t.Helper()
	env.writeRemoteFile(t, "package/nested/module.py", "package bytes")
	env.writeRemoteFile(t, "package/top.txt", "top-level bytes")
	if existing {
		if err := os.MkdirAll(filepath.Join(env.localRoot, "package/nested"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, rel := range []string{"package/nested", "package"} {
		if err := env.fsClient.Chmod(context.Background(), "/"+rel, 0o555); err != nil {
			t.Fatal(err)
		}
		if existing {
			if err := os.Chmod(filepath.Join(env.localRoot, rel), 0o555); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Cleanup(func() {
		for _, rel := range []string{"package", "package/nested"} {
			_ = os.Chmod(filepath.Join(env.localRoot, rel), 0o755)
		}
	})
}

func assertRecoveryDirectoryModes(t *testing.T, env *syncTestEnv) {
	t.Helper()
	for _, rel := range []string{"package", "package/nested"} {
		info, err := os.Stat(filepath.Join(env.localRoot, rel))
		if err != nil {
			t.Fatal(err)
		}
		remote, err := env.fsClient.Stat(context.Background(), "/"+rel)
		if err != nil || remote == nil {
			t.Fatalf("stat remote %s: %+v, %v", rel, remote, err)
		}
		if info.Mode().Perm() != 0o555 || remote.Mode != 0o555 {
			t.Fatalf("%s permissions: local=%o remote=%o, want 555", rel, info.Mode().Perm(), remote.Mode)
		}
	}
}

func TestSyncRecoveryPopulatesReadOnlyDirectories(t *testing.T) {
	for _, existing := range []bool{false, true} {
		name := "new"
		if existing {
			name = "existing"
		}
		t.Run(name, func(t *testing.T) {
			env, d := recoveryBaselineDiagnostic(t)
			seedRecoveryReadOnlyDirectories(t, env, existing)
			if err := d.full.warmStart(context.Background(), nil); err != nil {
				t.Fatalf("populate read-only directories: %v", err)
			}
			for rel, want := range map[string]string{"package/top.txt": "top-level bytes", "package/nested/module.py": "package bytes"} {
				if got := env.readLocalFile(t, rel); got != want {
					t.Fatalf("%s = %q, want %q", rel, got, want)
				}
			}
			assertRecoveryDirectoryModes(t, env)
			for _, rel := range []string{"package", "package/nested"} {
				if mode := d.Snapshot().Entries[rel].Mode; mode != 0o555 {
					t.Fatalf("persisted %s mode = %o, want 555", rel, mode)
				}
			}

			// A later remote update and delete have no mkdir action to reopen
			// their parent, whose mode already agrees with the baseline.
			env.writeRemoteFile(t, "package/nested/module.py", "updated package bytes")
			if err := env.fsClient.Rm(context.Background(), "/package/top.txt"); err != nil {
				t.Fatal(err)
			}
			if err := d.full.warmStart(context.Background(), nil); err != nil {
				t.Fatalf("update read-only directory: %v", err)
			}
			if got := env.readLocalFile(t, "package/nested/module.py"); got != "updated package bytes" {
				t.Fatalf("updated content = %q", got)
			}
			if env.localExists("package/top.txt") {
				t.Fatal("deleted remote file remains under read-only local parent")
			}
			assertRecoveryDirectoryModes(t, env)
		})
	}
}

type recoveryDirectoryReadClient struct {
	client.Client
	read func(context.Context, string) ([]byte, error)
}

func (c *recoveryDirectoryReadClient) Cat(ctx context.Context, path string) ([]byte, error) {
	return c.read(ctx, path)
}

func TestSyncRecoveryRestoresDirectoryModesAfterFailure(t *testing.T) {
	for _, cancelPass := range []bool{false, true} {
		name := "read_error"
		if cancelPass {
			name = "cancelled"
		}
		t.Run(name, func(t *testing.T) {
			env, d := recoveryBaselineDiagnostic(t)
			seedRecoveryReadOnlyDirectories(t, env, false)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			injected := errors.New("injected remote read failure")
			d.reconciler.fs = &recoveryDirectoryReadClient{Client: env.fsClient, read: func(context.Context, string) ([]byte, error) {
				if cancelPass {
					cancel()
					return nil, context.Canceled
				}
				return nil, injected
			}}
			wantErr := injected
			if cancelPass {
				wantErr = context.Canceled
			}
			if err := d.full.warmStart(ctx, nil); !errors.Is(err, wantErr) {
				t.Fatalf("failed pass = %v, want %v", err, wantErr)
			}
			assertRecoveryDirectoryModes(t, env)
			d.reconciler.fs = env.fsClient
			if err := d.full.warmStart(context.Background(), nil); err != nil {
				t.Fatalf("retry under restored read-only directories: %v", err)
			}
			if got := env.readLocalFile(t, "package/nested/module.py"); got != "package bytes" {
				t.Fatalf("retried content = %q", got)
			}
			assertRecoveryDirectoryModes(t, env)
		})
	}
}

func TestSyncRecoveryPreservesConcurrentDirectoryChmod(t *testing.T) {
	env, d := recoveryBaselineDiagnostic(t)
	seedRecoveryReadOnlyDirectories(t, env, true)
	d.reconciler.fs = &recoveryDirectoryReadClient{Client: env.fsClient, read: func(ctx context.Context, path string) ([]byte, error) {
		if path == "/package/nested/module.py" {
			if err := os.Chmod(filepath.Join(env.localRoot, "package/nested"), 0o700); err != nil {
				return nil, err
			}
		}
		return env.fsClient.Cat(ctx, path)
	}}
	if err := d.full.warmStart(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(env.localRoot, "package/nested"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("application chmod overwritten: mode=%o, want 700", info.Mode().Perm())
	}
}

func TestSyncRecoveryPreservesReplacementDirectoryMode(t *testing.T) {
	env, d := recoveryBaselineDiagnostic(t)
	abs := filepath.Join(env.localRoot, "directory")
	plan := []syncAction{{kind: "mkdir-local", path: "directory", absPath: abs, mode: 0o555}}
	var replaceErr error
	err := d.full.executePlan(context.Background(), plan, func(done, total int64) {
		if done != total {
			return
		}
		if replaceErr = os.Rename(abs, abs+"-moved"); replaceErr == nil {
			replaceErr = os.Mkdir(abs, 0o755)
		}
	})
	if err != nil || replaceErr != nil {
		t.Fatalf("reconcile=%v replace=%v", err, replaceErr)
	}
	info, err := os.Stat(abs)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("replacement directory chmod overwritten: mode=%o, want 755", info.Mode().Perm())
	}
	select {
	case <-d.reconciler.fullSweepRequests():
	default:
		t.Fatal("replacement directory did not request another reconciliation")
	}
}
