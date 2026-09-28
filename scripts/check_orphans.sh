#!/usr/bin/env bash
# After `terraform destroy`, look for billable leftovers. Fails if any are found.
set -u
PROJECT=ontario-grid-signal
found=0

for kind in disks forwarding-rules addresses; do
  leftovers=$(gcloud compute "$kind" list --project "$PROJECT" --format="value(name)")
  if [ -n "$leftovers" ]; then
    echo "LEFTOVER $kind:"
    echo "$leftovers"
    found=1
  fi
done

if [ "$found" -eq 0 ]; then
  echo "No orphans: no disks, forwarding rules or addresses."
fi
exit "$found"
