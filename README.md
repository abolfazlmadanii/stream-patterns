# stream-patterns

A catalog of streaming data modeling patterns for Apache Flink and Kafka, under development. Each pattern is planned to ship with a written explanation, the data model, an executable reference model, a Flink SQL implementation, tests, and its known failure modes.

**Status:** early. No patterns are published yet.

## Build and run

Requires JDK 17 and Maven 3.9+.

```bash
mvn -B verify
```

The default Flink version is defined in the root `pom.xml` (`flink.version`, currently 2.2.1). Override it for a one-off build against another release on Maven Central (for example Flink 2.3.0):

```bash
mvn -B verify -Dflink.version=2.3.0
```

CI also runs `mvn -B verify -Dflink.version=2.3.0` on JDK 17 as a forward-compatibility check; that matrix leg is allowed to fail (`continue-on-error`) until the repo fully tracks 2.3.x.

Tests will use [streamproof](https://github.com/abolfazlmadanii/streamproof), a companion correctness harness.

Licensed under the [Apache License 2.0](LICENSE).
