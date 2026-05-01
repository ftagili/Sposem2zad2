#!/usr/bin/env sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
COMPILER_DIR="${1:-${COMPILER_DIR:-}}"
PATCHED_CODEGEN="$SCRIPT_DIR/patched_source2ast_codegen.c"
PIPELINE_SRC="$SCRIPT_DIR/spo_task2_v18_pipeline.src"
WORKERS_SRC="$SCRIPT_DIR/spo_task2_v18_workers.src"
OUTPUT_DIR="$SCRIPT_DIR/output/from_src"

if [ -z "${COMPILER_DIR}" ]; then
  for cand in \
    "$SCRIPT_DIR/../task3_source2ast" \
    "$SCRIPT_DIR/../source2ast" \
    "$SCRIPT_DIR/../source2ast-41e8146cc0107ad31cf76932afc772daa74b4e0c"
  do
    if [ -d "$cand" ] && [ -f "$cand/CMakeLists.txt" ] && [ -d "$cand/src/codegen" ]; then
      COMPILER_DIR="$cand"
      break
    fi
  done
fi

if [ -z "${COMPILER_DIR}" ] || [ ! -d "$COMPILER_DIR" ] || [ ! -f "$COMPILER_DIR/CMakeLists.txt" ]; then
  echo "ERROR: compiler project not found."
  echo "Run like this:"
  echo "  ./run_task2_from_src_s390x.sh /path/to/source2ast"
  exit 2
fi

if [ ! -f "$PATCHED_CODEGEN" ]; then
  echo "ERROR: patched code generator file not found: $PATCHED_CODEGEN"
  exit 2
fi

TARGET_CODEGEN="$COMPILER_DIR/src/codegen/codegen.c"
BACKUP_CODEGEN="$TARGET_CODEGEN.bak.task2_variant18"

echo "[1/6] Sync patched code generator"
if ! cmp -s "$PATCHED_CODEGEN" "$TARGET_CODEGEN"; then
  if [ ! -f "$BACKUP_CODEGEN" ]; then
    cp "$TARGET_CODEGEN" "$BACKUP_CODEGEN"
  fi
  cp "$PATCHED_CODEGEN" "$TARGET_CODEGEN"
  echo "  patched: $TARGET_CODEGEN"
else
  echo "  already patched"
fi

echo "[2/6] Build compiler (source2ast)"
mkdir -p "$COMPILER_DIR/build"
cmake -S "$COMPILER_DIR" -B "$COMPILER_DIR/build"
cmake --build "$COMPILER_DIR/build"

echo "[3/6] Generate assembly from .src files"
mkdir -p "$OUTPUT_DIR"
mkdir -p "$SCRIPT_DIR/output"
"$COMPILER_DIR/build/codegen" "$PIPELINE_SRC" "$OUTPUT_DIR/spo_task2_v18_pipeline.s"
if "$COMPILER_DIR/build/codegen" "$WORKERS_SRC" "$OUTPUT_DIR/spo_task2_v18_workers.s"; then
  :
else
  rm -f "$OUTPUT_DIR/spo_task2_v18_workers.s"
  echo "  WARN: workers src asm generation skipped"
fi

echo "[4/6] Build runnable files"
cc -c -o "$OUTPUT_DIR/spo_task2_v18_pipeline.o" "$OUTPUT_DIR/spo_task2_v18_pipeline.s"
cc -std=c11 -O2 -Wall -Wextra -pedantic -c \
  -o "$OUTPUT_DIR/spo_task2_v18_src_bridge.o" "$SCRIPT_DIR/spo_task2_v18_src_bridge.c"
cc -std=c11 -O2 -Wall -Wextra -pedantic -c \
  -o "$OUTPUT_DIR/workload_s390x.o" "$SCRIPT_DIR/workload_s390x.S"
cc -no-pie \
  "$OUTPUT_DIR/spo_task2_v18_pipeline.o" \
  "$OUTPUT_DIR/spo_task2_v18_src_bridge.o" \
  "$OUTPUT_DIR/workload_s390x.o" \
  -o "$OUTPUT_DIR/spo_task2_v18_from_src" -lm

echo "  asm:    $OUTPUT_DIR/spo_task2_v18_pipeline.s"
if [ -f "$OUTPUT_DIR/spo_task2_v18_workers.s" ]; then
  echo "  asm:    $OUTPUT_DIR/spo_task2_v18_workers.s"
fi
echo "  object: $OUTPUT_DIR/spo_task2_v18_pipeline.o"
echo "  binary: $OUTPUT_DIR/spo_task2_v18_from_src"

echo "[5/6] Run generated program"
rm -f "$SCRIPT_DIR"/output/48_*.txt "$SCRIPT_DIR"/output/78_*.txt
set +e
(cd "$SCRIPT_DIR" && "$OUTPUT_DIR/spo_task2_v18_from_src")
RUN_STATUS=$?
set -e
if [ "$RUN_STATUS" -ne 0 ]; then
  echo "  WARN: generated program exited with status $RUN_STATUS"
fi

echo "[6/6] Show generated SQL-like result files"
for f in "$SCRIPT_DIR"/output/48_*.txt "$SCRIPT_DIR"/output/78_*.txt; do
  if [ ! -f "$f" ]; then
    continue
  fi
  printf '\n=== %s ===\n' "$(basename "$f")"
  if [ -s "$f" ]; then
    cat "$f"
  else
    echo "(no rows)"
  fi
done

exit "$RUN_STATUS"
