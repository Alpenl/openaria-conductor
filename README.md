# Open Aria Conductor

Open Aria Conductor is the device-side capture service for the D-Robotics
RDK X5 V1.0 and YLX 2UQ2 stereo camera. It records stereo video and IMU
samples, seals recording sessions, exposes a versioned device API, and serves
the embedded browser control surface.

The Python distribution and command remain named `rp-ylx` for the current
0.5 compatibility line.

## Requirements

- Python 3.11 or newer
- Node.js 22 for browser tests
- `uv`
- Linux for device deployment; hardware-free checks also run on GitHub-hosted
  Linux runners

## Build And Test

```bash
uv sync --frozen --extra dev
npm ci
uv run python scripts/check.py
npm run typecheck:web
npm run lint:web
npm run test:web
uv build
```

## Run Without Hardware

```bash
uv run rp-ylx --version
uv run rp-ylx status
uv run rp-ylx serve-mock
```

Use `uv run rp-ylx hardware-smoke --help` to inspect the explicit RDK X5
hardware smoke-test interface.

## License

MIT. See [LICENSE](LICENSE).
