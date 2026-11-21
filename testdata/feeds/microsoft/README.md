# Microsoft feed compatibility fixtures

The committed `original.xml` files are minimal, non-provider-specific
representative fixtures. They exercise fields and formats relevant to the six
configured endpoints without redistributing current Microsoft article text or
requiring network access in CI.

These files are not live captures and do **not** establish endpoint
compatibility. Each `capture.json` records that limitation explicitly. Real
compatibility is checked only when `RSSCOLLECTOR_LIVE_TESTS=1` is set; those
checks retain a sanitized report and do not commit downloaded response bodies.
