set -o pipefail
log="raft-flaky-$(date +%Y%m%d-%H%M%S).log"
i=1

while true; do
    echo "===== Run $i =====" | tee -a "$log"
    if ! go test -race -run '3A|3B' 2>&1 | tee -a "$log"; then
        echo "===== FAILED at run $i =====" | tee -a "$log"
        $ break
    fi
    i=$((i+1))
done