---
name: no-fatal-assertions
metadata:
  strict: true
  apply_to_path: 'squangle/.*\.(h|hh|hpp|hxx|c|cc|cpp|cxx|tcc)$'
---

# No fatal assertions in Squangle

**Do not add `CHECK`, `assert`, `LOG(FATAL)`, or any other process-killing
assertion to non-test Squangle code. Return an error instead.** This applies to
code you write and to code you modify.

Squangle is a client library linked into thousands of unrelated binaries. An
assertion that aborts here takes down someone else's process, not ours. And
because every one of those processes evaluates the same bad input, they all
abort at the same time. A returned error degrades one query.

## Banned outside tests

`CHECK` and all `CHECK_*` variants, `LOG(FATAL)`, `XLOG(FATAL)`, `XLOGF(FATAL)`,
`assert()`, `abort()`, `std::terminate()`, `folly::assume()`, and
`__builtin_unreachable()`. Also anything that reaches `std::terminate`, such as
throwing from a destructor or from a coroutine cleanup path.

## Use instead

- Return `folly::Expected<T, E>`, or throw a typed exception the caller can
  catch and handle.
- Log and keep going: `XLOG_EVERY_MS(ERR, 1000)` for a noisy error path.
- For "this should never happen", use `LOG(DFATAL)`. It is fatal in dev and CI
  but only an error log in release builds, so you get the signal without the
  outage.
- Better still, make the bad state unrepresentable: a `std::variant`, a strong
  type, or a private constructor beats a runtime check.

## Exceptions

Test code may assert freely. `static_assert` is compile-time and always fine.

`DCHECK` is allowed only for a true programming invariant, such as thread
affinity or a single-writer discipline, because it compiles out under `NDEBUG`.
Never `DCHECK` on anything derived from network input, server responses, or
configuration. Those are data-driven, and a `DCHECK` on them is a `CHECK` on
them in every dev and test build. If you cannot tell which kind it is, it is
data-driven; use `LOG(DFATAL)`.
