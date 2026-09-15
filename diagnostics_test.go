package fastsync

import (
	"net/rpc"
	"sync/atomic"
	"testing"
	"time"
)

func TestCPUAndPressureParsing(t *testing.T) {
	total, idle, wait, err := parseCPU("cpu  100 20 30 400 50 6 7 8 90 10\ncpu0 1 2 3 4\n")
	if err != nil || total != 621 || idle != 400 || wait != 50 {
		t.Fatalf("CPU parse: %d %d %d %v", total, idle, wait, err)
	}
	if _, _, _, err := parseCPU("cpu invalid 0 0 0"); err == nil {
		t.Fatal("invalid counters accepted")
	}
	if got := parsePressure("some avg10=37.25 avg60=20.0 total=123\nfull avg10=10.0\n"); got != 37.25 {
		t.Fatalf("pressure=%v", got)
	}
}

func TestBottleneckAttribution(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		local, remote         HostStatus
		remoteAvailable       bool
		localWork, remoteWork float64
		want                  string
	}{
		{name: "client IO", local: HostStatus{Valid: true, IOPressure: 70}, localWork: 12, want: "Client IO"},
		{name: "server IO", remote: HostStatus{Valid: true, IOWait: 40}, remoteAvailable: true, remoteWork: 3, want: "Server IO"},
		{name: "blocked client operation", local: HostStatus{Valid: true, IOPressure: 70, ActiveIO: 1}, want: "Client IO"},
		{name: "unrelated disk activity", local: HostStatus{Valid: true, IOPressure: 70}, want: "Unknown"},
		{name: "IO without host pressure", local: HostStatus{Valid: true}, localWork: 12, want: "Unknown"},
		{name: "stale server", remote: HostStatus{Valid: true, IOPressure: 70}, remoteAvailable: false, remoteWork: 3, want: "Unknown"},
		{name: "unsupported client", local: HostStatus{IOPressure: 70}, localWork: 3, want: "Unknown"},
		{name: "server CPU", remote: HostStatus{Valid: true, CPUBusy: 95, CPUPressure: 30}, remoteAvailable: true, want: "Server CPU"},
		{name: "client memory", local: HostStatus{Valid: true, MemoryPressure: 20}, want: "Client memory"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyBottleneck(tc.local, tc.remote, tc.remoteAvailable, tc.localWork, tc.remoteWork)
			if got.Label != tc.want {
				t.Fatalf("got %s (%s), want %s", got.Label, got.Reason, tc.want)
			}
		})
	}
}

func TestStatusRequiresHelloAndRoundTrips(t *testing.T) {
	server := NewServer()
	var status HostStatus
	if err := server.Status(struct{}{}, &status); err != ErrPleaseSayHello {
		t.Fatalf("pre-handshake status: %v", err)
	}
	registry := rpc.NewServer()
	registerTestRPCServer(t, registry, server)
	client := newTestRPCClientForServer(t, registry)
	if err := client.Call("Server.Hello", SharedOptions{ProtocolVersion: PROTOCOLVERSION}, nil); err != nil {
		t.Fatal(err)
	}
	server.localIO.Store(123456)
	server.activeIO.Store(3)
	if err := client.Call("Server.Status", struct{}{}, &status); err != nil {
		t.Fatal(err)
	}
	if status.Version != 1 || status.IONanoseconds != 123456 || status.ActiveIO != 3 {
		t.Fatalf("status lost counters: %+v", status)
	}
}

type delayedStatusServer struct {
	calls   atomic.Int64
	release chan struct{}
}

func (s *delayedStatusServer) Status(_ struct{}, reply *HostStatus) error {
	s.calls.Add(1)
	<-s.release
	*reply = HostStatus{Version: 1, Valid: true, IOPressure: 90, ActiveIO: 4}
	return nil
}
func TestStalledTelemetryDoesNotBlockShutdownOrQueueRequests(t *testing.T) {
	server := &delayedStatusServer{release: make(chan struct{})}
	defer close(server.release)
	registry := rpc.NewServer()
	registerTestRPCServer(t, registry, server)
	client := newTestClient(t.TempDir())
	stop := client.startDiagnostics(newTestRPCClientForServer(t, registry))
	time.Sleep(4200 * time.Millisecond)
	if calls := server.calls.Load(); calls != 1 {
		stop()
		t.Fatalf("stalled telemetry queued %d calls", calls)
	}
	if client.Diagnostics().ServerAvailable {
		stop()
		t.Fatal("stalled server reported available")
	}
	done := make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("telemetry blocked shutdown")
	}
}

type legacyStatusServer struct{}

func (*legacyStatusServer) Ping(_ struct{}, _ *struct{}) error { return nil }
func TestMissingStatusRPCIsOptional(t *testing.T) {
	registry := rpc.NewServer()
	registerTestRPCServer(t, registry, &legacyStatusServer{})
	client := newTestClient(t.TempDir())
	stop := client.startDiagnostics(newTestRPCClientForServer(t, registry))
	defer stop()
	time.Sleep(2200 * time.Millisecond)
	if client.Diagnostics().ServerAvailable {
		t.Fatal("legacy server reported status support")
	}
	if client.runError() != nil {
		t.Fatal("optional telemetry failed transfer")
	}
}

func TestDurableIsOptIn(t *testing.T) {
	if NewClient().Durable {
		t.Fatal("durability must be opt-in")
	}
	for _, durable := range []bool{false, true} {
		source, dest := t.TempDir(), t.TempDir()
		writeTestFile(t, source, "file", "backup content")
		runTestSync(t, source, dest, func(c *Client) { c.Durable = durable })
		if got := readTestFile(t, dest, "file"); got != "backup content" {
			t.Fatalf("durable=%v: %q", durable, got)
		}
	}
}
