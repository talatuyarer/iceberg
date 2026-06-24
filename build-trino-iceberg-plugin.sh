#!/usr/bin/env bash
#
# build-trino-iceberg-plugin.sh
#
# Build a Trino Iceberg connector plugin against a LOCALLY-PATCHED Apache Iceberg.
#
# Workflow:
#   1. Resolve the target Trino version (arg/env, or auto-detect the latest release).
#   2. Build & publish the patched Iceberg (this repo) to ~/.m2 under a custom version.
#   3. Clone/checkout Trino at the target tag.
#   4. Build the trino-iceberg plugin with -Ddep.iceberg.version pointing at the patched jars.
#   5. Print the resulting plugin zip path.
#
# Re-run for any Trino version, e.g.:   ./build-trino-iceberg-plugin.sh 481
#
# Configurable via env vars (see the table below). Defaults target Trino's latest release.
#
set -euo pipefail

# ----------------------------------------------------------------------------
# Configuration (override via environment)
# ----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Trino git tag to build. Positional arg wins, then $TRINO_VERSION, else auto-detect.
TRINO_VERSION="${1:-${TRINO_VERSION:-}}"

# This Iceberg repo (defaults to the directory containing this script).
ICEBERG_DIR="${ICEBERG_DIR:-$SCRIPT_DIR}"

# Version label used when publishing the patched Iceberg to ~/.m2.
ICEBERG_PUBLISH_VERSION="${ICEBERG_PUBLISH_VERSION:-1.10.1-rest-pointer}"

# Where to clone/build Trino (next to the iceberg repo for easy inspection/editing).
TRINO_DIR="${TRINO_DIR:-$HOME/Workspace/trino}"

# JDKs: Iceberg needs 11/17/21 (NOT 25); Trino needs a recent JDK (24/25).
ICEBERG_JAVA_HOME="${ICEBERG_JAVA_HOME:-$(/usr/libexec/java_home -v 17 2>/dev/null || true)}"
TRINO_JAVA_HOME="${TRINO_JAVA_HOME:-$(/usr/libexec/java_home -v 25 2>/dev/null || true)}"

# Set SKIP_ICEBERG_PUBLISH=1 to reuse already-published patched jars.
SKIP_ICEBERG_PUBLISH="${SKIP_ICEBERG_PUBLISH:-}"

# Iceberg modules that trino-iceberg depends on (Gradle pulls their deps automatically).
# These are the Gradle project paths (settings.gradle renames each project to its 'iceberg-*' id).
ICEBERG_MODULES=(
  ":iceberg-bundled-guava"
  ":iceberg-api"
  ":iceberg-common"
  ":iceberg-core"
  ":iceberg-parquet"
  ":iceberg-orc"
  ":iceberg-aws"
  ":iceberg-azure"
  ":iceberg-gcp"
  ":iceberg-nessie"
  ":iceberg-snowflake"
)

# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[warn]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }

# ----------------------------------------------------------------------------
# Step 1: Resolve Trino version
# ----------------------------------------------------------------------------
if [[ -z "$TRINO_VERSION" ]]; then
  log "No Trino version given; detecting latest release from GitHub..."
  TRINO_VERSION="$(curl -fsSL https://api.github.com/repos/trinodb/trino/releases/latest \
    | grep -m1 '"tag_name"' \
    | sed -E 's/.*"tag_name"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/')"
  [[ -n "$TRINO_VERSION" ]] || die "Could not auto-detect the latest Trino release. Pass one explicitly: $0 <version>"
fi
log "Target Trino version: $TRINO_VERSION"

# ----------------------------------------------------------------------------
# Step 2: Validate JDKs
# ----------------------------------------------------------------------------
[[ -n "$ICEBERG_JAVA_HOME" && -x "$ICEBERG_JAVA_HOME/bin/java" ]] \
  || die "Iceberg JDK not found. Install JDK 17 (or 11/21) and set ICEBERG_JAVA_HOME. (Iceberg fails on JDK 25.)"
[[ -n "$TRINO_JAVA_HOME"   && -x "$TRINO_JAVA_HOME/bin/java" ]] \
  || die "Trino JDK not found. Install the JDK required by Trino $TRINO_VERSION and set TRINO_JAVA_HOME."

log "Iceberg build JDK: $("$ICEBERG_JAVA_HOME/bin/java" -version 2>&1 | head -1) ($ICEBERG_JAVA_HOME)"
log "Trino build JDK:   $("$TRINO_JAVA_HOME/bin/java" -version 2>&1 | head -1) ($TRINO_JAVA_HOME)"

# ----------------------------------------------------------------------------
# Step 3: Build & publish the patched Iceberg to ~/.m2
# ----------------------------------------------------------------------------
if [[ -n "$SKIP_ICEBERG_PUBLISH" ]]; then
  log "SKIP_ICEBERG_PUBLISH set; reusing existing ~/.m2 jars for $ICEBERG_PUBLISH_VERSION"
else
  [[ -x "$ICEBERG_DIR/gradlew" ]] || die "No gradlew in ICEBERG_DIR ($ICEBERG_DIR)"

  # Override the published version by writing version.txt; restore it on exit so the repo stays clean.
  VERSION_TXT="$ICEBERG_DIR/version.txt"
  VERSION_TXT_BACKUP=""
  if [[ -f "$VERSION_TXT" ]]; then
    VERSION_TXT_BACKUP="$(mktemp)"
    cp "$VERSION_TXT" "$VERSION_TXT_BACKUP"
  fi
  restore_version_txt() {
    if [[ -n "$VERSION_TXT_BACKUP" ]]; then
      mv "$VERSION_TXT_BACKUP" "$VERSION_TXT"
    else
      rm -f "$VERSION_TXT"
    fi
  }
  trap restore_version_txt EXIT

  printf '%s\n' "$ICEBERG_PUBLISH_VERSION" > "$VERSION_TXT"
  log "Publishing patched Iceberg as version '$ICEBERG_PUBLISH_VERSION' to ~/.m2 ..."

  publish_tasks=()
  for m in "${ICEBERG_MODULES[@]}"; do publish_tasks+=("${m}:publishToMavenLocal"); done

  ( cd "$ICEBERG_DIR" && JAVA_HOME="$ICEBERG_JAVA_HOME" ./gradlew \
      -x test -x integrationTest "${publish_tasks[@]}" )

  # Restore version.txt now that publishing is done; drop the trap.
  restore_version_txt
  trap - EXIT
  log "Patched Iceberg published: ~/.m2/repository/org/apache/iceberg/*/$ICEBERG_PUBLISH_VERSION/"
fi

# ----------------------------------------------------------------------------
# Step 4: Clone/checkout Trino at the target tag
# ----------------------------------------------------------------------------
if [[ -z "${SKIP_TRINO_CHECKOUT:-}" ]]; then
  if [[ -d "$TRINO_DIR/.git" ]]; then
    log "Updating existing Trino checkout at $TRINO_DIR ..."
    git -C "$TRINO_DIR" fetch --depth 1 origin "refs/tags/$TRINO_VERSION:refs/tags/$TRINO_VERSION" 2>/dev/null \
      || git -C "$TRINO_DIR" fetch origin "$TRINO_VERSION"
    git -C "$TRINO_DIR" checkout -f "$TRINO_VERSION"
  else
    log "Cloning Trino $TRINO_VERSION into $TRINO_DIR ..."
    mkdir -p "$(dirname "$TRINO_DIR")"
    git clone --depth 1 --branch "$TRINO_VERSION" https://github.com/trinodb/trino.git "$TRINO_DIR"
  fi
else
  log "SKIP_TRINO_CHECKOUT set; skipping checkout for Trino version $TRINO_VERSION"
fi

# Warn if Trino's pinned Iceberg major.minor differs from our patched base.
TRINO_ICEBERG_VER="$(grep -m1 '<dep.iceberg.version>' "$TRINO_DIR/pom.xml" 2>/dev/null \
  | sed -E 's/.*<dep.iceberg.version>([^<]+)<.*/\1/' || true)"
if [[ -n "$TRINO_ICEBERG_VER" ]]; then
  trino_mm="${TRINO_ICEBERG_VER%.*}"
  ours_mm="${ICEBERG_PUBLISH_VERSION%.*}"; ours_mm="${ours_mm%-*}"
  log "Trino $TRINO_VERSION pins Iceberg $TRINO_ICEBERG_VER; overriding with $ICEBERG_PUBLISH_VERSION"
  if [[ "$trino_mm" != "$ours_mm" ]]; then
    warn "Iceberg major.minor mismatch (Trino wants $trino_mm.x, patched is $ours_mm.x)."
    warn "Trino may fail to compile if the Iceberg API drifted between these versions."
  fi
fi

# ----------------------------------------------------------------------------
# Step 5: Build the trino-iceberg plugin against the patched Iceberg
# ----------------------------------------------------------------------------
log "Building trino-iceberg plugin (this can take a while) ..."
( cd "$TRINO_DIR" && JAVA_HOME="$TRINO_JAVA_HOME" ./mvnw -B install \
    -pl plugin/trino-iceberg -am -DskipTests \
    -Dair.check.skip-all=true \
    -Dair.compiler.fail-warnings=false \
    -Dmaven.source.skip=true -Dmaven.javadoc.skip=true \
    -Ddep.iceberg.version="$ICEBERG_PUBLISH_VERSION" ) \
  || die "Trino build failed. If the JDK was rejected, install the JDK Trino $TRINO_VERSION requires and set TRINO_JAVA_HOME."

# Notes:
# -Dmaven.test.skip=true skips test compile/run entirely (we only want the plugin artifact);
#   it also sidesteps the JDK-vs-generated-protobuf -Werror issue in upstream test sources.
# -Dair.compiler.fail-warnings=false additionally relaxes -Werror for newer JDKs.

# ----------------------------------------------------------------------------
# Step 6: Report output
# ----------------------------------------------------------------------------
PLUGIN_ZIP="$(ls -t "$TRINO_DIR"/plugin/trino-iceberg/target/trino-iceberg-*.zip 2>/dev/null | head -1 || true)"
echo
log "Done."
if [[ -n "$PLUGIN_ZIP" ]]; then
  log "Plugin zip:     $PLUGIN_ZIP"
  log "Exploded dir:   ${PLUGIN_ZIP%.zip}/"
  echo
  echo "Install into a Trino server:"
  echo "  rm -rf <TRINO_SERVER>/plugin/iceberg"
  echo "  unzip -q -j \"$PLUGIN_ZIP\" -d <TRINO_SERVER>/plugin/iceberg"
else
  warn "Build finished but no plugin zip was found under plugin/trino-iceberg/target/"
fi
