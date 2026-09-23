package fastsync

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"net/rpc"
	"testing"
)

func authenticateTestSession(t *testing.T, s *Server) {
	t.Helper()
	var ch AuthChallenge
	if err := s.Challenge(struct{}{}, &ch); err != nil {
		t.Fatal(err)
	}
	if err := s.Authenticate(AuthRequest{Versions: CurrentVersions()}, &AuthReply{}); err != nil {
		t.Fatal(err)
	}
}
func authTestRPC(t *testing.T, s *Server) *rpc.Client {
	t.Helper()
	r := rpc.NewServer()
	registerTestRPCServer(t, r, s)
	return newTestRPCClientForServer(t, r)
}
func TestPasswordHandshakeAndCopy(t *testing.T) {
	for _, pair := range [][2]string{{"", ""}, {"synthetic-test-credential", "synthetic-test-credential"}, {"synthetic-test-credential", "wrong"}, {"synthetic-test-credential", ""}, {"", "unexpected"}} {
		t.Run(pair[0]+"/"+pair[1], func(t *testing.T) {
			source := t.TempDir()
			writeTestFile(t, source, "server-a/file", "sample")
			listener := NewServer()
			listener.BasePath = source
			listener.ConfigurePassword(pair[0])
			s := listener.NewSession()
			defer s.CloseFiles()
			c := newTestClient(t.TempDir())
			c.Password = pair[1]
			c.SourcePath = "server-a"
			err := c.Run(authTestRPC(t, s))
			if pair[0] != pair[1] {
				if err == nil || s.clientsaidhello.Load() {
					t.Fatal("accepted invalid credentials")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := readTestFile(t, c.BasePath, "file"); got != "sample" {
				t.Fatal(got)
			}
			// Verification uses the same authentication path on a fresh session.
			v := newTestClient(c.BasePath)
			v.Password = pair[1]
			v.SourcePath = "server-a"
			var report bytes.Buffer
			if err := v.Verify(authTestRPC(t, listener.NewSession()), &report); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestAuthenticationGatesAndReplay(t *testing.T) {
	listener := NewServer()
	listener.ConfigurePassword("synthetic-test-credential")
	a, b := listener.NewSession(), listener.NewSession()
	for _, s := range []*Server{a, b} {
		if err := s.SelectRoot(".", nil); !errors.Is(err, ErrAuthentication) {
			t.Fatalf("root before authentication: %v", err)
		}
		if err := s.Hello(NewClient().Options, nil); !errors.Is(err, ErrAuthentication) {
			t.Fatalf("hello before authentication: %v", err)
		}
		if err := s.Shutdown(nil, nil); !errors.Is(err, ErrPleaseSayHello) {
			t.Fatal(err)
		}
	}
	var ca, cb AuthChallenge
	a.Challenge(struct{}{}, &ca)
	b.Challenge(struct{}{}, &cb)
	if bytes.Equal(ca.Nonce, cb.Nonce) {
		t.Fatal("reused nonce")
	}
	key := sha256.Sum256([]byte("synthetic-test-credential"))
	proof := AuthRequest{Versions: CurrentVersions(), Proof: authProof(key[:], ca.Nonce, "client", CurrentVersions())}
	if err := a.Authenticate(proof, &AuthReply{}); err != nil {
		t.Fatal(err)
	}
	if err := a.Authenticate(proof, &AuthReply{}); !errors.Is(err, ErrAuthentication) {
		t.Fatal("same-session replay")
	}
	if err := b.Authenticate(proof, &AuthReply{}); !errors.Is(err, ErrAuthentication) {
		t.Fatal("cross-session replay")
	}
	if err := b.Challenge(struct{}{}, &cb); !errors.Is(err, ErrAuthentication) {
		t.Fatal("retry after failure")
	}
	if b.authenticated {
		t.Fatal("authentication leaked between sessions")
	}
}
func TestCompatibilityVersions(t *testing.T) {
	for _, v := range []VersionInfo{{}, {1, BEHAVIORVERSION}, {PROTOCOLVERSION - 1, BEHAVIORVERSION}, {PROTOCOLVERSION, 0}, {PROTOCOLVERSION, BEHAVIORVERSION - 1}, {PROTOCOLVERSION, BEHAVIORVERSION + 1}, {PROTOCOLVERSION + 1, BEHAVIORVERSION}} {
		s := NewServer()
		authenticateTestSession(t, s)
		if err := s.Hello(SharedOptions{ProtocolVersion: v.ProtocolVersion, BehaviorVersion: v.BehaviorVersion}, &VersionInfo{}); err == nil {
			t.Fatalf("accepted %+v", v)
		}
		if s.clientsaidhello.Load() {
			t.Fatal("mismatch enabled archive RPCs")
		}
	}
}

type mismatchedChallengeServer struct{ *Server }

func (s *mismatchedChallengeServer) Challenge(_ struct{}, reply *AuthChallenge) error {
	if err := s.Server.Challenge(struct{}{}, reply); err != nil {
		return err
	}
	reply.Versions.BehaviorVersion++
	return nil
}

type legacyHandshakeServer struct{}

func (*legacyHandshakeServer) Hello(_ SharedOptions, _ *VersionInfo) error { return nil }
func TestClientRejectsIncompatibleAndLegacyServers(t *testing.T) {
	mismatch := &mismatchedChallengeServer{NewServer()}
	for _, server := range []any{mismatch, &legacyHandshakeServer{}} {
		registry := rpc.NewServer()
		registerTestRPCServer(t, registry, server)
		if err := NewClient().Handshake(newTestRPCClientForServer(t, registry)); err == nil {
			t.Fatal("accepted incompatible handshake")
		}
	}
	if mismatch.authAttempted {
		t.Fatal("sent authentication to incompatible server")
	}
}
