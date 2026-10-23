# Architecture decisions (stream-patterns)

## ADR-006: Reference model language

Each pattern ships a **plain Python** reference model that defines correct behavior independently of Flink SQL or PyFlink operators. Tests compare pipeline output to that reference via the `testkit.Oracle` protocol.
