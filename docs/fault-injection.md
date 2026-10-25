# Fault Injection Matrix

This document describes the conformance fault matrix used by `greentic-runner`.

## Fault matrix file

A fault matrix file is JSON with a list of cases. Each case targets a pack/flow
and describes which fault(s) to inject plus expected behavior.

Example:

```json
{
  "cases": [
    {
      "name": "component_timeout_once_then_success",
      "pack": "messaging-dummy",
      "flow": "setup",
      "faults": [
        {
          "point": "BeforeComponentCall",
          "mode": "Once",
          "error_kind": "Timeout",
          "message": "simulated timeout"
        }
      ],
      "expect": {
        "retries": 1,
        "status": "pass",
        "dlq": false
      }
    }
  ]
}
```

### Supported fault points

- `BeforeComponentCall`
- `AfterComponentCall`
- `BeforeToolCall`
- `AfterToolCall`
- `StateRead`
- `StateWrite`
- `TemplateRender`
- `PackResolve`
- `Timeout`

### Fault modes

- `Always`
- `Once`
- `Nth`
- `Rate`

### Error kinds

- `Transient`
- `Permanent`
- `Timeout`
- `Trap`
- `BadInput`

## Running locally

Build with the fault injection feature and run conformance with a matrix file:

```bash
cargo run -p greentic-runner --features fault-injection -- \
  conformance --packs tests/fixtures/packs --level l2 \
  --faults tests/fixtures/faults/basic.json --trace
```

## Artifacts

When a fault case produces unexpected results, artifacts are written under:

```
target/fault-artifacts/<case>/<timestamp>/
```

Contents:
- `faults.json`
- `inputs.json`
- `result.json`
- `trace.txt` (if trace output was available)
