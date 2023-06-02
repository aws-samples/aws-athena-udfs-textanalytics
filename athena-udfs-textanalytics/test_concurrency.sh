PAYLOAD="My name is Bob. I live in Herndon."
TOTAL_REQUESTS=20000
for PARTITION_COUNT in 1 5 10 20; do
    REQ_PER_PARTITION=$((TOTAL_REQUESTS / PARTITION_COUNT))
    echo "================================================================================================="
    echo "$PARTITION_COUNT concurrent batches, $REQ_PER_PARTITION requests per batch = $TOTAL_REQUESTS requests"
    echo "================================================================================================="
    start=$(date +%s)
    for i in $(seq $PARTITION_COUNT)
    do
        echo "Starting partition: $i"
        java -cp target/textanalyticsudfs-1.0.jar com.amazonaws.athena.udf.textanalytics.TextAnalyticsUDFHandler perftest $REQ_PER_PARTITION "$PAYLOAD" > /tmp/out &
        PIDS[${i}]=$!
    done

    for pid in ${PIDS[*]}; do
        wait $pid
    done
    end=$(date +%s)
    ELAPSED_TIME=$(($end-$start))
    REQUESTS_PER_SEC=$(echo "scale=2 ; $TOTAL_REQUESTS / $ELAPSED_TIME" | bc)

    echo "Done all batches.. Processed $TOTAL_REQUESTS in $ELAPSED_TIME seconds."
    echo "Requests/sec: $REQUESTS_PER_SEC"
done

