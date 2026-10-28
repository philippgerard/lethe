# Migrator compression dependency patches

These are the published crates.io sources for arrow-ipc 58.4.0 and
lance-encoding 6.0.0, including their upstream license files.
Only each normalized Cargo.toml dependency on zstd changes: 0.13 to 0.14.3.
Cargo.lock and Cargo cache markers are omitted; the migrator owns resolution.

Both upstream releases require the affected zstd 0.13 / zstd-safe 7 stack.
Using zstd >=0.14.3 and zstd-safe >=8.2.0 addresses the published RustSec
compression advisories without upgrading the legacy storage readers.
No advisories are ignored and all three release lockfile audits remain required.

Remove these patches when compatible upstream releases adopt fixed Zstandard.
Validate with the migrator round-trip suite and upstream compression tests.

Upstream archive SHA-256:

- `arrow-ipc-58.4.0.crate`: `29a908a11fcfb3fb2f6730f4ac15e367bc644e419155e96238f68cf3adde572b`
- `lance-encoding-6.0.0.crate`: `b4386ea75123fe6194e166f0f1d375b6041d44507fe7b7c63ac0da493de78a2f`
