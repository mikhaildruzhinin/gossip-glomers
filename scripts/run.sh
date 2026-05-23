#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT" || exit 1

mvn -q compile dependency:build-classpath \
  -Dmdep.outputFile=target/classpath.txt \
  >/dev/null

exec java -cp "target/classes:$(cat target/classpath.txt)" ru.mikhaildruzhinin.gossipglomers.Main
