set -o pipefail

log="raft-$(date +%Y%m%d-%H%M%S).log"
i=1

while true; do
    echo "===== Run $i =====" | tee -a "$log"

    if ! { time go test -run '3D'; } 2>&1 | tee -a "$log"; then
        echo "===== FAILED at run $i =====" | tee -a "$log"
        # break
    fi

    ((i++))
done