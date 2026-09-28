<!--
Copyright (c) 2026 ADBC Drivers Contributors
Copyright (c) 2026 Query Farm LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Worker SDK behavior coverage

Equal test counts are not the contract. One Go test can exercise several backend
hooks, while the TypeScript suite separates those hooks and tests its Arrow
runtime separately. Adding empty or duplicate cases would not establish parity.
Every implementation should instead satisfy the same observable requirements,
with additional tests for its own runtime and hosting code.

The common external suite gives Go and TypeScript the same test inputs through
the ordinary native ADBC driver. `contract.json` separately checks the exact
31-method inventory and 18 named record schemas. A missing-session error for a
method proves request decoding and ownership enforcement; it does **not** prove
that method's positive backend behavior. The SDK tests below supply that check.

| Required behavior | Go SDK tests | TypeScript SDK tests |
| --- | --- | --- |
| Exact authoritative schemas and registrations | `contract_test.go` | `service.test.ts`: authoritative methods; `check:contract` |
| Transactions, preparation, schema, update, Substrait | `TestFullOptionalBackendDispatch` | dispatches preparation/schema/update/Substrait/transactions |
| Scalar typed options and configured-option authority | `TestOptionValidationAndServerAuthority`, `TestAuthoritativeOptionsRejectAcrossScopes` | option APIs; authoritative options |
| Binding, streamed binding, ingestion dispatch | `TestBindingAndIngestionDispatch` | binding stages; bind_stream |
| Metadata and statistics hooks | `TestFullOptionalBackendDispatch` | metadata hooks |
| Partition ownership and tamper rejection | `TestPartitionOwnershipTamperingAndExpiry` | partition tokens |
| Handle ownership, pull replay, parent cleanup | common external suite | common external suite; cross-principal/domain and pull replay SDK tests |
| Cancellation while an operation is busy | `TestCancelBypassesBusySession` | hosting shutdown cancels active backend work |
| Structured errors, repeated binary details, private-message redaction | backend-error and error-details tests | ADBC errors preserve details and sanitize exceptions |
| Idle reaping and callbacks completing after shutdown | `TestIdleReaping`, `TestShutdownReapsLateOpen` | idle reaping; busy shutdown |
| Session/result quotas, invalid row counts and retained Arrow allocations | `lifecycle_test.go`, allocator boundary | quota/binding boundary tests; `arrow.test.ts` |
| Framing and nested uncompressed Arrow records | `TestStrictNamedCodec` | `arrow.test.ts` |
| Host admission, transport identity, disconnects, deadlines | `transport_test.go` | hosting and transport tests |

These are pointers to evidence, not a claim that every SDK unit-test branch is
identical. For example, a shutdown-triggered cancellation test does not replace
every explicit statement/connection cancellation scenario. Keep tests for those
distinctions even when implementations need different fixtures or case counts.

## Transport selection

All transports run native ADBC checks for exact results, batch boundaries,
repeated execution, partial reads, error recovery, independent statements,
concurrent connections, interleaved live results from two clients, bounded
session churn with a live observer, and target authorization. Additional checks
depend on
the authentication mechanism and framing:

| Checks | HTTP / HTTPS | TCP | mTLS TCP | Iroh |
| --- | --- | --- | --- | --- |
| Native ADBC common behavior | Yes | Yes | Yes | Yes |
| Repeated complete/abandoned persistent pulls | Common repeated-query checks | Extended | Extended | Extended |
| Independent typed wire requests, schema/version rejection | Yes | Unary | Unary | Native driver only |
| Missing-session request inventory | All 30 handle methods | All 27 unary handle methods | All 27 unary handle methods | SDK tests |
| Explicit wire sequence replay | Yes | SDK tests | SDK tests | SDK tests |
| Wrong bearer credentials | Yes | Not an identity mechanism | Not an identity mechanism | Not an identity mechanism |
| Two authorized identities and denied third identity | Bearers | One configured local identity | Verified certificates | Authenticated endpoint IDs |
| Cross-owner handles through independently encoded requests | Bearers | Same local identity | Verified certificates | SDK identity tests; no independent Iroh wire client yet |
| CA and hostname validation | HTTPS; hostname also native Rust regression | Not applicable | Yes | Cryptographic endpoint identity |
| Disconnect/malformed stream recovery | HTTP framework and SDK tests | Yes | Yes | SDK/bridge cleanup plus native repeated pulls |
| Shutdown live-handle accounting | Yes | Yes | Yes | Every fixture audits shutdown; focused host tests |

Transport-specific deselection is reported explicitly. It is not a skipped
failure or evidence of full wire-fault parity across transports. In particular,
an independent raw-Iroh malformed-request client remains a coverage gap.

The Python and Rust reference workers have their own suites. The Rust example
supports two distinct HTTP and mTLS principals for the shared ownership gate.
Earlier HTTP reference runs are recorded in `RESULTS.md`; rerunning the new
Go/TypeScript transport matrix does not retroactively validate either reference
worker.
None of these unit or conformance counts establish production soak, RSS
stability, callback hard termination, or cross-process session portability.
