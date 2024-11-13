#!/bin/bash

if [ $# -eq 1 ] && [ "$1" = "--help" ]; then
    echo "Usage: $0 [RUNS=100] [PARALLELISM=#cpus] [TESTPATTERN='']"
    exit 1
fi

if ! go test -c -v -race -o tester ./raft; then
    echo -e "\e[1;31mERROR: Build failed\e[0m"
    exit 1
fi

runs=100
if [ $# -gt 0 ]; then
    runs="$1"
fi

parallelism=$(sysctl -n hw.ncpu)
if [ $# -gt 1 ]; then
    parallelism="$2"
fi

test=""
if [ $# -gt 2 ]; then
    test="$3"
fi

logs=$(find . -maxdepth 1 -name 'test-*.log' -type f | wc -l)
success=$(grep -E '^PASS$' test-*.log 2>/dev/null | wc -l)
((failed = logs - success))

finish() {
    if ! wait "$1"; then
        if command -v notify-send >/dev/null 2>&1 && ((failed == 0)); then
            notify-send -i weather-storm "Tests started failing" \
                "$(pwd)\n$(grep FAIL: -- *.log | sed -e 's/.*FAIL: / - /' -e 's/ (.*)//' | sort -u)"
        fi
        ((failed += 1))
    else
        ((success += 1))
    fi

    if [ "$failed" -eq 0 ]; then
        printf "\e[1;32m";
    else
        printf "\e[1;31m";
    fi

    printf "Done %03d/%d; %d ok, %d failed\n\e[0m" \
        $((success+failed)) \
        "$runs" \
        "$success" \
        "$failed"
}

waits=()
is=()

cleanup() {
    for pid in "${waits[@]}"; do
        kill "$pid"
        wait "$pid"
        rm -rf "test-${is[0]}.err" "test-${is[0]}.log"
        is=("${is[@]:1}")
    done
    exit 0
}
trap cleanup SIGHUP SIGINT SIGTERM

for i in $(seq "$((success+failed+1))" "$runs"); do
    if [[ ${#waits[@]} -eq "$parallelism" ]]; then
        finish "${waits[0]}"
        waits=("${waits[@]:1}")
        is=("${is[@]:1}")
    fi

    is=("${is[@]}" $i)

    if [[ -z "$test" ]]; then
        ./tester -test.v 2> "test-${i}.err" > "test-${i}.log" &
    else
        ./tester -test.run "$test" -test.v 2> "test-${i}.err" > "test-${i}.log" &
    fi
    pid=$!
    waits=("${waits[@]}" $pid)
done

for pid in "${waits[@]}"; do
    finish "$pid"
done

if ((failed > 0)); then
    exit 1
fi
exit 0

