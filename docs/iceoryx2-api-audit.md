# iceoryx2 API Audit

## Status

- Current baseline: `eclipse-iceoryx/iceoryx2` commit `3107941ba2a40f2897c395289447d0f93664ad8c`.
- `crates/core` has no `iceoryx2` dependency.
- `crates/cli` has no direct `iceoryx2` dependency.
- `crates/iceoryx2` contains all transport-specific integration.

## Required iceoryx2 Surfaces

- Public pub-sub open/create/subscribe/publish APIs for dynamic payload services.
- Public request-response APIs for recorder control.
- Public access to service `MessageTypeDetails` for recording existing services.
- Dynamic payload loan/send APIs via `CustomPayloadMarker`.

## Non-Stable-Looking Surfaces

The adapter still needs runtime construction of arbitrary `TypeDetail` layouts
when rematerializing archived bytes into a dynamic pub-sub service. Upstream
currently exposes this through:

- `iceoryx2::testing::type_detail_set_size`
- `iceoryx2::testing::type_detail_set_alignment`
- `iceoryx2::testing::type_detail_set_name`
- `__internal_set_payload_type_details`
- `__internal_set_user_header_type_details`

Upstream's own `iox2-service publish` command uses the same pattern for dynamic
service tooling, so this is not unique to this archive project.

## Isolation Point

Runtime `TypeDetail` construction is isolated in:

- `crates/iceoryx2/src/dynamic_type.rs`

Hidden builder overrides remain in:

- `crates/iceoryx2/src/record.rs`
- `crates/iceoryx2/src/rematerialize/pubsub.rs`

## Upstream API Request

The stable API needed by external recorder/replay tooling is a public way to:

1. Construct `TypeDetail` from `(type_name, variant, size, alignment)`.
2. Open/create dynamic pub-sub services using caller-provided payload and user-header `TypeDetail`.

Once available, replace `dynamic_type.rs` and the two hidden builder override
call sites without changing archive core, SQLite, or CLI logic.
