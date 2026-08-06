#!/usr/bin/env bash
#
# Deletes per-node kahuna.log files from Jepsen run directories.
#
# Kahuna logs at debug level, so a five-node run drops ~70 MB of server logs
# into store/<test>/<timestamp>/n*/kahuna.log. A few dozen runs fill a disk.
#
# Those logs are the difference between "the checker says invalid" and knowing
# why — one hypothesis in this project was disproved purely by the *absence* of
# a log line across 400k debug lines. So this prunes conservatively and keeps
# them wherever they might still be read:
#
#   * failing runs        — the evidence for every open investigation
#   * runs still writing  — anything touched in the last hour
#   * runs with no verdict yet — crashed or in-flight
#
# Everything else is a passing run whose server logs nobody will ever open.
# The history, results.edn, timeline and plots are never touched, so a pruned
# run is still fully re-analyzable — only the server-side logs go.
#
# Usage:
#   scripts/prune-logs.sh              # prune
#   scripts/prune-logs.sh --dry-run    # show what would go

set -euo pipefail

cd "$(dirname "$0")/.."

DRY=0
[ "${1:-}" = "--dry-run" ] && DRY=1

[ -d store ] || { echo "no store/ directory; nothing to do"; exit 0; }

kept=0 pruned=0 kb=0

for d in store/*/*/; do
  [ -d "$d" ] || continue
  # store/latest and store/current are symlinks to a run; skip so a run is
  # never counted (or pruned) twice under two names.
  [ -L "${d%/}" ] && continue
  case "$d" in store/latest/*|store/current/*) continue;; esac

  # Still being written? Leave it alone. NOTE: -mmin, not -newermt — the `find`
  # on some systems here is bfs, which rejects -newermt and would otherwise make
  # this guard silently match nothing.
  if find "$d" -maxdepth 1 -mmin -60 -print -quit 2>/dev/null | grep -q .; then
    kept=$((kept + 1)); continue
  fi

  # No verdict written: crashed or interrupted. Keep — these are the ones worth
  # looking at when a run "disappears".
  if [ ! -f "$d/results.edn" ]; then
    kept=$((kept + 1)); continue
  fi

  # Failing run: this is evidence. Keep.
  if grep -q ":valid? false" "$d/results.edn" 2>/dev/null; then
    kept=$((kept + 1)); continue
  fi

  sz=$(find "$d" -name kahuna.log -type f -exec du -ck {} + 2>/dev/null | tail -1 | cut -f1)
  [ -z "${sz:-}" ] && sz=0
  [ "$sz" -eq 0 ] && continue   # already pruned

  if [ "$DRY" -eq 1 ]; then
    echo "would prune $((sz / 1024)) MB from $d"
  else
    find "$d" -name kahuna.log -type f -delete 2>/dev/null || true
  fi
  pruned=$((pruned + 1)); kb=$((kb + sz))
done

verb=$([ "$DRY" -eq 1 ] && echo "would reclaim" || echo "reclaimed")
echo "kept $kept run(s) (failing / recent / no verdict)"
echo "pruned $pruned passing run(s), $verb $((kb / 1024)) MB"
[ "$DRY" -eq 0 ] && echo "store is now $(du -sh store 2>/dev/null | cut -f1)"
exit 0
