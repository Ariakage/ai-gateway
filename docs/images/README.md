# Screenshot provenance

These images show the actual embedded AI Gateway dashboard, captured with headless Chromium / Playwright on 2026-10-30.

| File | View |
| --- | --- |
| `dashboard-light.png` | Chinese dashboard, light theme |
| `dashboard-dark.png` | Chinese dashboard, dark theme |
| `routing.png` | Route price cards, cache ratios, latency, and recommendations |

The gateway ran with a temporary copy of `dev/test-config.toml` and a local `dev/mock_upstream.py` server. A separate temporary SQLite database contained actual mock requests plus **synthetic historical demonstration rows** to populate the time-series chart. Reported traffic, prices, latency, and throughput are illustrative; they are not live-provider measurements or performance claims.

No private configuration, real provider credentials, personal conversation history, or production usage database was used. The images were optimized as PNG without changing the dashboard's layout or displayed metrics.
