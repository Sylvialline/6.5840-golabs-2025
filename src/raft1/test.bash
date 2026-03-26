set -o pipefail

log="raft-$(date +%Y%m%d-%H%M%S).log"
i=1
test="3D"

echo Test case: \"$test\"

while true; do
    echo "===== Run $i =====" | tee -a "$log"

    if ! { time go test -run $test; } 2>&1 | tee -a "$log"; then
        echo "===== FAILED at run $i =====" | tee -a "$log"
        break
    fi

    ((i++))
done