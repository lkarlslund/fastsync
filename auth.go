package fastsync

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/rpc"
)

var ErrAuthentication = errors.New("authentication failed")

type VersionInfo struct{ ProtocolVersion, BehaviorVersion int }

func CurrentVersions() VersionInfo { return VersionInfo{PROTOCOLVERSION, BEHAVIORVERSION} }
func (v VersionInfo) check() error {
	if v != CurrentVersions() {
		return fmt.Errorf("incompatible fastsync versions: peer protocol=%d behavior=%d, local protocol=%d behavior=%d; upgrade both endpoints", v.ProtocolVersion, v.BehaviorVersion, PROTOCOLVERSION, BEHAVIORVERSION)
	}
	return nil
}

type AuthChallenge struct {
	Versions VersionInfo
	Required bool
	Nonce    []byte
}
type AuthRequest struct {
	Versions VersionInfo
	Proof    []byte
}
type AuthReply struct{ Proof []byte }

// ConfigurePassword must run before accepting connections. The password is never
// placed in RPC options or logs. Empty explicitly disables password protection.
func (s *Server) ConfigurePassword(password string) {
	if password == "" {
		s.passwordKey = nil
		return
	}
	key := sha256.Sum256([]byte(password))
	s.passwordKey = key[:]
}
func authProof(key, nonce []byte, role string, v VersionInfo) []byte {
	mac := hmac.New(sha256.New, key)
	fmt.Fprintf(mac, "fastsync-auth-v1/%s/%d/%d/", role, v.ProtocolVersion, v.BehaviorVersion)
	mac.Write(nonce)
	return mac.Sum(nil)
}
func (s *Server) Challenge(_ struct{}, reply *AuthChallenge) error {
	s.helloMu.Lock()
	defer s.helloMu.Unlock()
	if s.authAttempted || s.clientsaidhello.Load() {
		return ErrAuthentication
	}
	if s.authNonce == nil {
		s.authNonce = make([]byte, 32)
		if _, err := rand.Read(s.authNonce); err != nil {
			s.authNonce = nil
			return err
		}
	}
	*reply = AuthChallenge{Versions: CurrentVersions(), Required: len(s.passwordKey) > 0, Nonce: append([]byte(nil), s.authNonce...)}
	return nil
}
func (s *Server) Authenticate(request AuthRequest, reply *AuthReply) error {
	s.helloMu.Lock()
	defer s.helloMu.Unlock()
	if s.authAttempted || len(s.authNonce) != 32 || s.clientsaidhello.Load() {
		return ErrAuthentication
	}
	s.authAttempted = true // One attempt per connection, including a failed version check.
	nonce := s.authNonce
	s.authNonce = nil
	if err := request.Versions.check(); err != nil {
		return err
	}
	if len(s.passwordKey) > 0 {
		if !hmac.Equal(request.Proof, authProof(s.passwordKey, nonce, "client", request.Versions)) {
			return ErrAuthentication
		}
		reply.Proof = authProof(s.passwordKey, nonce, "server", request.Versions)
	} else if len(request.Proof) != 0 {
		return ErrAuthentication
	}
	s.authenticated = true
	return nil
}

// Handshake checks compatibility in both directions and authenticates before
// selecting a source. It deliberately never falls back to legacy RPCs.
func (c *Client) Handshake(client *rpc.Client) error {
	var challenge AuthChallenge
	if err := client.Call("Server.Challenge", struct{}{}, &challenge); err != nil {
		return fmt.Errorf("handshake challenge (both endpoints need current handshake support): %w", err)
	}
	if err := challenge.Versions.check(); err != nil {
		return err
	}
	if len(challenge.Nonce) != 32 {
		return errors.New("invalid authentication challenge")
	}
	if challenge.Required != (c.Password != "") {
		return errors.New("password configuration mismatch: configure the same password on both endpoints")
	}
	request := AuthRequest{Versions: CurrentVersions()}
	var key [32]byte
	if challenge.Required {
		key = sha256.Sum256([]byte(c.Password))
		request.Proof = authProof(key[:], challenge.Nonce, "client", request.Versions)
	}
	var authenticated AuthReply
	if err := client.Call("Server.Authenticate", request, &authenticated); err != nil {
		return fmt.Errorf("authenticate: %w", err)
	}
	if challenge.Required && !hmac.Equal(authenticated.Proof, authProof(key[:], challenge.Nonce, "server", request.Versions)) {
		return ErrAuthentication
	}
	if c.SourcePath != "" {
		if err := client.Call("Server.SelectRoot", c.SourcePath, nil); err != nil {
			return fmt.Errorf("select source %q: %w", c.SourcePath, err)
		}
	}
	var versions VersionInfo
	if err := client.Call("Server.Hello", c.Options, &versions); err != nil {
		return fmt.Errorf("hello: %w", err)
	}
	return versions.check()
}
