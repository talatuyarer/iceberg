#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# ==============================================================================
# setup_local_trino_biglake.sh
#
# Bootstrap and configure a local Trino server running our custom built Iceberg
# plugin, pointing to a BigQuery BigLake REST catalog in GCP.
#
# Usage:
#   ./setup_local_trino_biglake.sh <GCP_PROJECT_ID> <GCS_BUCKET_NAME> [TRINO_VERSION]
#
# Example:
#   ./setup_local_trino_biglake.sh metastore-migration-joonix metastore-migration-joonix-talat 481
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PARENT_DIR="$(dirname "${SCRIPT_DIR}")"

# --- Argument Parsing & Validation ---
PURGE=false
POSITIONAL_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --purge|-p)
      PURGE=true
      shift
      ;;
    -*)
      echo "❌ Error: Unknown option $1"
      echo "Usage: $0 [--purge] <GCP_PROJECT_ID> <GCS_BUCKET_NAME> [TRINO_VERSION]"
      exit 1
      ;;
    *)
      POSITIONAL_ARGS+=("$1")
      shift
      ;;
  esac
done
if [[ "${#POSITIONAL_ARGS[@]}" -gt 0 ]]; then
  set -- "${POSITIONAL_ARGS[@]}"
else
  set --
fi

if [[ "$#" -lt 2 ]]; then
  echo "❌ Error: Missing required arguments."
  echo "Usage: $0 [--purge] <GCP_PROJECT_ID> <GCS_BUCKET_NAME> [TRINO_VERSION]"
  exit 1
fi

PROJECT_ID="$1"
BUCKET_NAME="$2"
TRINO_VERSION="${3:-481}"

TRINO_SERVER_DIR="${PARENT_DIR}/trino-server-${TRINO_VERSION}"
DOWNLOADS_DIR="${SCRIPT_DIR}/.trino-downloads"
TRINO_TARBALL="${DOWNLOADS_DIR}/trino-server-${TRINO_VERSION}.tar.gz"

# Helpers
log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[warn]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }
if [[ "$PURGE" = true ]]; then
  log "Purging existing Trino server directory and downloaded tarball..."
  rm -rf "$TRINO_SERVER_DIR"
  rm -f "$TRINO_TARBALL"
fi

log "Setting up local Trino ${TRINO_VERSION} for BigLake catalog '${BUCKET_NAME}'..."
# --- Step 0: Validate Prerequisites ---
command -v docker >/dev/null 2>&1 || die "docker is required but not installed. Please install Docker."
command -v npm >/dev/null 2>&1 || die "npm is required but not installed. Please install Node/npm."

# --- Step 1: Validate JDKs ---
# Trino 481 requires JDK 24 or 25; Iceberg requires JDK 17 (or 11/21, NOT 25)
ICEBERG_JAVA_HOME="${ICEBERG_JAVA_HOME:-$(/usr/libexec/java_home -v 17 2>/dev/null || true)}"
TRINO_JAVA_HOME="${TRINO_JAVA_HOME:-$(/usr/libexec/java_home -v 25 2>/dev/null || true)}"

# Fallbacks if versions not found
if [[ -z "$ICEBERG_JAVA_HOME" ]]; then
  ICEBERG_JAVA_HOME="$(/usr/libexec/java_home -v 21 2>/dev/null || true)"
fi
if [[ -z "$TRINO_JAVA_HOME" ]]; then
  TRINO_JAVA_HOME="$(/usr/libexec/java_home -v 24 2>/dev/null || true)"
fi

[[ -n "$ICEBERG_JAVA_HOME" && -x "$ICEBERG_JAVA_HOME/bin/java" ]] \
  || die "Iceberg build JDK not found. Install JDK 17 or 21 and set ICEBERG_JAVA_HOME."
[[ -n "$TRINO_JAVA_HOME" && -x "$TRINO_JAVA_HOME/bin/java" ]] \
  || die "Trino build JDK not found. Install JDK 24 or 25 and set TRINO_JAVA_HOME."

# Export them for the child build script
export ICEBERG_JAVA_HOME
export TRINO_JAVA_HOME

# --- Step 2: Download or Build Trino Server Tarball ---
if [[ -d "$TRINO_SERVER_DIR" ]]; then
  log "Trino server directory already exists at: ${TRINO_SERVER_DIR}"
else
  mkdir -p "$DOWNLOADS_DIR"
  if [[ ! -f "$TRINO_TARBALL" ]]; then
    log "Downloading Trino server ${TRINO_VERSION} tarball..."
    if ! curl -fsSL -o "$TRINO_TARBALL" \
      "https://repo1.maven.org/maven2/io/trino/trino-server/${TRINO_VERSION}/trino-server-${TRINO_VERSION}.tar.gz"; then
      warn "Trino server version ${TRINO_VERSION} was not found on Maven Central. Attempting to build from source..."

      # Clone/checkout Trino if not done already
      TRINO_SRC_DIR="${PARENT_DIR}/trino"
      if [[ ! -d "$TRINO_SRC_DIR/.git" ]]; then
        log "Cloning Trino ${TRINO_VERSION} into ${TRINO_SRC_DIR}..."
        mkdir -p "$(dirname "$TRINO_SRC_DIR")"
        git clone --depth 1 --branch "$TRINO_VERSION" https://github.com/trinodb/trino.git "$TRINO_SRC_DIR" \
          || git clone https://github.com/trinodb/trino.git "$TRINO_SRC_DIR"
        git -C "$TRINO_SRC_DIR" checkout "$TRINO_VERSION"
      fi

      log "Building Trino server from source (this can take a while)..."
      # Find if core/trino-server or server/trino-server exists
      TRINO_SRV_MODULE="core/trino-server"
      TRINO_CORE_MODULE="core/trino-server-core"
      if [[ ! -d "${TRINO_SRC_DIR}/${TRINO_SRV_MODULE}" ]]; then
        TRINO_SRV_MODULE="server/trino-server"
        TRINO_CORE_MODULE="server/trino-server-core"
      fi
      [[ -d "${TRINO_SRC_DIR}/${TRINO_SRV_MODULE}" ]] || die "Could not find Trino server module directory inside source."

      # Resolve the hdfs plugin directory (could be lib/trino-hdfs or plugin/trino-hdfs in different versions)
      TRINO_HDFS_MODULE="lib/trino-hdfs"
      if [[ ! -d "${TRINO_SRC_DIR}/${TRINO_HDFS_MODULE}" ]]; then
        TRINO_HDFS_MODULE="plugin/trino-hdfs"
      fi

      PROVISIO_XML="${TRINO_SRC_DIR}/${TRINO_SRV_MODULE}/src/main/provisio/trino.xml"
      log "Stripping unused plugins from provisio assembly config: ${PROVISIO_XML}"

      python3 -c "
import xml.etree.ElementTree as ET
import sys
file_path = sys.argv[1]
tree = ET.parse(file_path)
root = tree.getroot()
for artifact_set in list(root.findall('artifactSet')):
    to_attr = artifact_set.get('to', '')
    if to_attr not in ['', 'plugin/iceberg']:
        root.remove(artifact_set)
tree.write(file_path, encoding='utf-8', xml_declaration=True)
" "$PROVISIO_XML"

      log "Building minimal Trino server and required plugins from source..."
      ( cd "$TRINO_SRC_DIR" && JAVA_HOME="$TRINO_JAVA_HOME" ./mvnw install \
          -pl "${TRINO_SRV_MODULE},${TRINO_CORE_MODULE},plugin/trino-iceberg,${TRINO_HDFS_MODULE}" -am -DskipTests \
          -Dair.check.skip-all=true \
          -Dair.compiler.fail-warnings=false \
          -Dmaven.source.skip=true -Dmaven.javadoc.skip=true )

      # Revert the provisio XML changes to clean up git state in the source directory
      log "Restoring original provisio assembly config..."
      git -C "$TRINO_SRC_DIR" checkout -- "${TRINO_SRV_MODULE}/src/main/provisio/trino.xml"

      # Find the generated tar.gz
      BUILT_TARBALL="$(find "${TRINO_SRC_DIR}/${TRINO_SRV_MODULE}/target" -name "trino-server-*.tar.gz" | head -1 || true)"
      [[ -n "$BUILT_TARBALL" && -f "$BUILT_TARBALL" ]] || die "Failed to build Trino server from source."

      cp "$BUILT_TARBALL" "$TRINO_TARBALL"
      log "Trino server built and saved to ${TRINO_TARBALL}"
    fi
  fi

  log "Extracting Trino server to ${PARENT_DIR}..."
  tar -xzf "$TRINO_TARBALL" -C "$PARENT_DIR"

  # If the extracted directory name differs (e.g. has SNAPSHOT suffix), rename it to match TRINO_SERVER_DIR
  EXTRACTED_DIR="$(find "$PARENT_DIR" -maxdepth 1 -type d -name "trino-server-*" ! -name "trino-server-${TRINO_VERSION}" | head -1 || true)"
  if [[ -n "$EXTRACTED_DIR" ]]; then
    log "Renaming ${EXTRACTED_DIR} to ${TRINO_SERVER_DIR}"
    mv "$EXTRACTED_DIR" "$TRINO_SERVER_DIR"
  fi
fi

# --- Step 3: Compile and Publish Patched Iceberg & Trino Plugin ---
log "Building and packaging custom Trino Iceberg plugin..."
./build-trino-iceberg-plugin.sh "${TRINO_VERSION}"

# Resolve the output plugin zip location
PLUGIN_ZIP_SOURCE="${PARENT_DIR}/trino/plugin/trino-iceberg/target/trino-iceberg-${TRINO_VERSION}.zip"
if [[ ! -f "$PLUGIN_ZIP_SOURCE" ]]; then
  PLUGIN_ZIP_SOURCE="$(ls -t "${PARENT_DIR}"/trino/plugin/trino-iceberg/target/trino-iceberg-*.zip 2>/dev/null | head -1 || true)"
fi

[[ -f "$PLUGIN_ZIP_SOURCE" ]] || die "Failed to locate built trino-iceberg plugin zip."

# --- Step 4: Inject Custom Plugin into Trino Server ---
log "Deploying custom Iceberg plugin into Trino server..."
rm -rf "${TRINO_SERVER_DIR}/plugin/iceberg"
mkdir -p "${TRINO_SERVER_DIR}/plugin/iceberg"
unzip -q -j "$PLUGIN_ZIP_SOURCE" -d "${TRINO_SERVER_DIR}/plugin/iceberg"

# --- Step 5: Generate Configuration Files ---
log "Generating configuration files under ${TRINO_SERVER_DIR}/etc..."
mkdir -p "${TRINO_SERVER_DIR}/etc"
mkdir -p "${TRINO_SERVER_DIR}/etc/catalog"

# 5a. etc/node.properties
cat <<EOF > "${TRINO_SERVER_DIR}/etc/node.properties"
node.environment=development
node.id=$(uuidgen)
node.data-dir=${TRINO_SERVER_DIR}/data
EOF

# 5b. etc/jvm.config
cat <<EOF > "${TRINO_SERVER_DIR}/etc/jvm.config"
-server
-XX:+UnlockExperimentalVMOptions
-Xmx16G
-XX:InitialRAMPercentage=60
-XX:MaxRAMPercentage=60
-XX:G1ReservePercent=15
-XX:InitiatingHeapOccupancyPercent=40
-XX:G1MixedGCLiveThresholdPercent=90
-XX:+UseG1GC
-XX:G1HeapRegionSize=32m
-XX:+ExplicitGCInvokesConcurrent
-XX:+ExitOnOutOfMemoryError
-XX:+HeapDumpOnOutOfMemoryError
-XX:-OmitStackTraceInFastThrow
-XX:ReservedCodeCacheSize=512M
-XX:PerMethodRecompilationCutoff=10000
-XX:PerBytecodeRecompilationCutoff=10000
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2048
-XX:+UnlockDiagnosticVMOptions
-XX:+UseAES
-XX:+UseAESIntrinsics
EOF

# 5c. etc/config.properties
cat <<EOF > "${TRINO_SERVER_DIR}/etc/config.properties"
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
query.max-memory=4GB
query.max-memory-per-node=1GB
discovery.uri=http://localhost:8080
log.enable-console=true
EOF

# 5d. etc/catalog/iceberg.properties
# Resolve GCP ADC credentials path
ADC_PATH="${HOME}/.config/gcloud/application_default_credentials.json"
ADC_CONFIG_LINE=""
if [[ -f "$ADC_PATH" ]]; then
  ADC_CONFIG_LINE="gcs.json-key-file-path=${ADC_PATH}"
fi

cat <<EOF > "${TRINO_SERVER_DIR}/etc/catalog/iceberg.properties"
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://biglake.googleapis.com/iceberg/v1beta/restcatalog
iceberg.rest-catalog.warehouse=gs://${BUCKET_NAME}
iceberg.rest-catalog.security=GOOGLE
iceberg.rest-catalog.google-project-id=${PROJECT_ID}
iceberg.rest-catalog.view-endpoints-enabled=false
iceberg.unique-table-location=false
fs.gcs.enabled=true
gcs.auth-type=APPLICATION_DEFAULT
${ADC_CONFIG_LINE}
EOF

log "Configurations generated successfully!"

# --- Step 6: Verify and Instruct ---
echo
echo "======================================================================"
echo "🎉 Setup Complete!"
echo "======================================================================"
echo "To start your local Trino server, run:"
echo
echo -e "  \033[1;32mJAVA_HOME=\"${TRINO_JAVA_HOME}\" ${TRINO_SERVER_DIR}/bin/launcher run\033[0m"
echo
# Resolve available CLI version (fallback to 476 if target version is not on Central)
CLI_VERSION="${TRINO_VERSION}"
if ! curl -fsSLI "https://repo1.maven.org/maven2/io/trino/trino-cli/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}-executable.jar" >/dev/null 2>&1; then
  CLI_VERSION="476"
fi

echo "To connect to the server once it is running, download the Trino CLI:"
echo "  curl -s -o trino-cli https://repo1.maven.org/maven2/io/trino/trino-cli/${CLI_VERSION}/trino-cli-${CLI_VERSION}-executable.jar"
echo "  chmod +x trino-cli"
echo "  ./trino-cli --server http://localhost:8080"
echo
echo "Verification Queries (inside Trino CLI):"
echo "  CREATE TABLE iceberg.test_schema.test_table (id bigint, val varchar);"
echo "  INSERT INTO iceberg.test_schema.test_table VALUES (1, 'hello');"
echo "  SELECT * FROM iceberg.test_schema.test_table;"
echo "======================================================================"
echo
