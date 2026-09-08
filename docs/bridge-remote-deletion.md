# Bridge Remote Recording Deletion

Implemented on the isolated `codex/bridge-remote-deletion` branch, based on
`b18be3c`. The canonical Conductor checkout and its active development branch
were not edited or switched. No device firmware or real recordings were modified
during validation.

## API

Device API v4 advertises `capabilities.session_deletion: true`. Older device
descriptors with `false` remain valid. Versions v2 and v3 are unchanged.

`POST /api/v4/sessions/delete` requires the `deleteSessions` operation permission
and the existing write authentication policy. Customer requests require Bearer,
Origin, and CSRF headers. All requests require an `Idempotency-Key`.

```json
{
  "schema": "ylx.session-delete-request.v1",
  "sessions": [
    {
      "session_id": "01989f6a-2c00-7a1b-8c2d-3e4f50617283",
      "manifest_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }
  ]
}
```

The request accepts 1-200 distinct UUIDv7 recording IDs. There is no wildcard
selection and no implicit deletion of continuations. Every selected digest and
continuation dependency is checked before any deletion. Invalid or ambiguous
catalog directories block deletion rather than guessing their dependencies.

The coordinator serializes deletion with capture and volume operations. Capture,
finalization, active manifest/artifact downloads, and volume release reject the
request. Directory handles pin the admitted roots, symlinks are rejected, and
recording directory identities are checked again before removal. Continuations
are removed before predecessors. A filesystem error stops further removal and
returns successes plus failures; the batch is not a rollback transaction.

```json
{
  "schema": "ylx.session-delete-result.v1",
  "deleted_session_ids": ["01989f6a-2c00-7a1b-8c2d-3e4f50617283"],
  "failed_sessions": []
}
```

Completed results use the existing principal-scoped persistent command cache
(last 1024 commands). Same key and body replay the original result, including
across service restart. A conflicting body returns 409. Power loss before a
result is persisted can leave a partially removed recording: refresh the catalog
before deciding on a new deletion. This feature does not claim transactional
recovery from power loss or modify local Bridge exports.

## Client Integration

Bridge enables its existing deletion confirmation for capable LAN devices. It
sends the displayed manifest digests and reuses the same idempotency key on
transport retries. Customer CSRF defaults to the configured Bearer token, with
`OPENARIA_DEVICE_CSRF_TOKEN` available for a separate device CSRF secret.

Echo Web previously rejected descriptors with `session_deletion: true`. The
companion isolated branch `codex/remote-deletion-compat` in Echo Web changes that
field to a boolean and updates its contract fingerprint. Conductor contains the
rebuilt `app.js` and `assets.json`; the Web source is maintained in Echo Web, not
patched in the generated bundle. No new Web deletion UI is introduced.

## Validation

- Gateway tests cover unauthorized, forbidden, CSRF rejection, malformed and
  duplicate selections, and forwarding an accepted command.
- Coordinator tests cover selected-only removal, restart replay, active capture,
  open downloads, stale digests, continuation dependencies, symlinks, partial
  storage failure, and the legacy sessions root.
- Bridge tests cover identical-key transport retries, permanent rejection,
  response reconciliation, stale confirmation, cache invalidation, old firmware,
  and enabling the TUI action only when supported.
- `tools/verify_remote_deletion.py` in Bridge runs a real local customer-profile
  gateway backed by temporary test recordings. It removes one recording, retains
  another, simulates a lost successful response, and verifies idempotent replay.

Deployment and physical-device deletion acceptance are separate from these
local checks. Integrate the isolated branch into the agreed firmware branch
before building and deploying an RDK bundle.
