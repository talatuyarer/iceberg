# Build the patched Iceberg plugin and run Trino with it

End-to-end runbook to build a Trino Iceberg connector plugin against the locally-patched
`rest-metadata-by-pointer` Iceberg, and run a Trino server that uses it.

## Version matrix (why these versions)

| Component | Version | Notes |
|-----------|---------|-------|
| Patched Iceberg | `1.10.1-rest-pointer` | published to `~/.m2`; based on Iceberg 1.10.1 |
| Trino | **481** | pins Iceberg 1.10.1 — matches the patch base |
| Build JDK (Iceberg) | 17 | Iceberg fails on JDK 25 |
| Build + run JDK (Trino) | 25 | Trino 481 requires JDK 25 |

> A Trino plugin must match the server version **exactly** (shared SPI). Trino 481 is not a prebuilt
> download on Maven Central (only ≤476 is, and 476 pins Iceberg 1.9.1 + needs JDK 24), so we use a
> **Trino 481 server** obtained from Docker or built from source — never mix versions.

## Prerequisites

- JDK **17** and JDK **25** installed (`/usr/libexec/java_home -v 17` / `-v 25` resolve them).
- This Iceberg repo at `~/Workspace/iceberg`, Trino cloned at `~/Workspace/trino` (the build script
  clones it for you).
- For the easy install path: **Docker**. For the no-Docker path: ability to run a large Maven build.

---

## Step 1 — Build the plugin (patched Iceberg + `trino-iceberg` zip)

From the Iceberg repo, run the helper script. It publishes the patched Iceberg to `~/.m2` (JDK 17),
clones/checks out Trino 481, and builds the plugin against it (JDK 25):

```bash
cd ~/Workspace/iceberg
./build-trino-iceberg-plugin.sh 481
```

Output (printed at the end):
- Patched jars in `~/.m2/repository/org/apache/iceberg/*/1.10.1-rest-pointer/`
- Plugin zip: `~/Workspace/trino/plugin/trino-iceberg/target/trino-iceberg-481.zip`

Unzip it into a directory of jars (this *is* the connector plugin):

```bash
rm -rf /tmp/iceberg-plugin && mkdir -p /tmp/iceberg-plugin
unzip -q ~/Workspace/trino/plugin/trino-iceberg/target/trino-iceberg-481.zip -d /tmp/iceberg-plugin
# -> /tmp/iceberg-plugin/trino-iceberg-481/   (contains iceberg-core-1.10.1-rest-pointer.jar, etc.)
PLUGIN_DIR=/tmp/iceberg-plugin/trino-iceberg-481
```

---

## Step 2 — Prepare catalog config

Create an Iceberg REST catalog config pointing at your REST catalog server. The metadata-by-pointer
feature is **on by default**; toggle it with `iceberg.rest-catalog.metadata-by-reference.enabled` if
your client exposes it (otherwise it follows the iceberg-core default).

```bash
mkdir -p /tmp/trino-etc/catalog
cat > /tmp/trino-etc/catalog/iceberg.properties <<'EOF'
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://host.docker.internal:8181
# Local-filesystem warehouse for a quick test; use s3://… / gs://… in real deployments.
fs.native-local.enabled=true
EOF
```

> You need a running Iceberg **REST catalog server** at that URI for the connector to talk to.
> For real use, point at your catalog. For a throwaway local one, any REST catalog implementation
> works (e.g. an Iceberg REST adapter over JDBC). The connector is the patched client either way.

---

## Step 3 — Run Trino 481 with the patched plugin

### Path A — Docker (recommended, no server build)

Run the official `trinodb/trino:481` image, replacing its bundled iceberg plugin with the rebuilt one
and mounting the catalog config:

```bash
docker run --rm -it --name trino \
  -p 8080:8080 \
  -v "$PLUGIN_DIR":/usr/lib/trino/plugin/iceberg:ro \
  -v /tmp/trino-etc/catalog:/etc/trino/catalog:ro \
  trinodb/trino:481
```

Trino is ready when the log prints `======== SERVER STARTED ========`.

### Path B — Server tarball from source (no Docker)

Build the full Trino 481 server distribution (bundles the patched iceberg plugin via the same Iceberg
override). This is a large one-time build:

```bash
cd ~/Workspace/trino
JAVA_HOME=$(/usr/libexec/java_home -v 25) ./mvnw -B install \
  -pl core/trino-server -am -Dmaven.test.skip=true \
  -Dair.check.skip-all=true -Dair.compiler.fail-warnings=false \
  -Ddep.iceberg.version=1.10.1-rest-pointer

tar -xzf core/trino-server/target/trino-server-481.tar.gz -C /tmp
SERVER=/tmp/trino-server-481

# minimal config
mkdir -p "$SERVER/etc/catalog"
cp /tmp/trino-etc/catalog/iceberg.properties "$SERVER/etc/catalog/"
cat > "$SERVER/etc/node.properties" <<EOF
node.environment=test
node.id=trino-local
node.data-dir=$SERVER/data
EOF
cat > "$SERVER/etc/jvm.config" <<'EOF'
-server
-Xmx4G
--add-opens=java.base/java.nio=ALL-UNNAMED
EOF
cat > "$SERVER/etc/config.properties" <<'EOF'
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
EOF

JAVA_HOME=$(/usr/libexec/java_home -v 25) "$SERVER/bin/launcher" run
```

---

## Step 4 — Verify the connector works

Use the Trino CLI (download `trino-cli-481-executable.jar` from `core/trino-cli/target/` after the
source build, or `docker exec -it trino trino` with the Docker path):

```sql
SHOW CATALOGS;                              -- includes "iceberg"
CREATE SCHEMA iceberg.demo;
CREATE TABLE iceberg.demo.t (id bigint, name varchar);
INSERT INTO iceberg.demo.t VALUES (1,'a'), (2,'b'), (3,'c');
SELECT count(*) FROM iceberg.demo.t;        -- 3
SELECT * FROM iceberg.demo.t ORDER BY id;   -- exercises the metadata-pointer commit + read path
```

`CREATE TABLE` + `INSERT` + `SELECT` all working confirms the patched client/server pointer path
(the same behavior covered by `TestIcebergRestMetadataPointer`).

---

## Re-running for a different Trino version

```bash
./build-trino-iceberg-plugin.sh <version>     # e.g. 480
```
Then use the matching `trinodb/trino:<version>` image / `trino-server-<version>.tar.gz`. If the target
Trino pins a different Iceberg major.minor than `1.10.1`, rebase the patch onto that Iceberg first
(the script warns about a mismatch).
