set -o pipefail

i=1
test="3D"
log="raft-${test}-$(date +%Y%m%d-%H%M%S).log"

echo Test case: \"$test\"

while true; do
    echo "===== Run $i =====" | tee -a "$log"

    if ! { time go test -race -run $test; } 2>&1 | tee -a "$log"; then
        echo "===== FAILED at run $i =====" | tee -a "$log"
        # break
    fi

    ((i++))
done