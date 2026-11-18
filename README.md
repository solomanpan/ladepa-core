# ladepa-core

The shared kernel of [ladepa](../ladepa) (the Rust twin of `kimoj-core` / the Go `core`): the pieces every service built on the same DDD layering needs.

* `exception` — `Kind` (400/401/403/404/409), `DomainError` (a business failure, a value), `InfraError` (technical failure; `upstream` → 502), `AppError`.
* `factory` — `ErrorSpec`: define every business error once (`conflict("system_role", "system role '{code}' cannot be deleted")`); templates carry named parameters so messages can be translated without parsing English.
* `rctx` — task-local `RequestContext` (request id, operator, locale) and deterministic id derivation (uuid v5) so ids can be known before an asynchronous write happens.
* `facade` — traits only: `UnitOfWork` / `Session` (`in_master`, `in_replica`, after-commit `defer`), `Publisher` / `MessageBus`, `Cache`, `PasswordHasher`.
* `memory` — `MemoryBus` (ordered, panic-safe, `drain()` for tests) and `MemoryCache`.

```toml
ladepa-core = { git = "https://github.com/solomanpan/ladepa-core", tag = "v1.0.0" }
```

`cargo test` · MIT © 2026 PAN JIE
