# FastSync development rules

## Compatibility versions

Any change to protocol or externally observable transfer behavior **requires a
bump to `PROTOCOLVERSION` in `shared.go`**. This includes RPC arguments/replies,
handshake or authentication changes, archive metadata interpretation, checksum
and reuse rules, traversal or hardlink semantics, verification, and durability or
error/completion semantics. Do not leave the protocol version unchanged merely
because a message can still be decoded.

For behavioral changes, also bump `BEHAVIORVERSION`. Both versions must match
exactly on both endpoints during the handshake. Reject missing or incompatible
versions before archive access; never silently downgrade or fall back to an old
handshake. Add or update compatibility tests with each bump.

Pure presentation changes (TUI colors/layout/text), documentation, and internal
refactoring that preserve protocol and observable transfer behavior do not
require a version bump. Explain that classification when it could be ambiguous.

## Privacy and validation

Use generic hostnames, example.com addresses, and synthetic paths/data in tests
and documentation. Never commit passwords, keys, private infrastructure names,
internal addresses, operational logs, or deployment configuration.

Run relevant tests and `go vet ./...`. Run `go test -race ./...` for concurrency,
handshake, and transfer changes. Do not deploy or restart running transfers
unless the user authorizes that action.
