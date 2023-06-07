PAYLOAD="My name is Bob. I live in Herndon."
TOTAL_REQUESTS=1000

PAYLOAD_BYTES=${#PAYLOAD}
for PARTITION_COUNT in 20; do
    REQ_PER_PARTITION=$((TOTAL_REQUESTS / PARTITION_COUNT))
    echo "================================================================================================="
    echo "$PARTITION_COUNT concurrent batches, $REQ_PER_PARTITION requests per batch = $TOTAL_REQUESTS requests"
    echo "================================================================================================="
    start=$(date +%s)
    for i in $(seq $PARTITION_COUNT)
    do
        OUTFILE=/tmp/out_total_${TOTAL_REQUESTS}_partitions_${PARTITION_COUNT}_partition_${i}.txt
        #OUTFILE=/dev/null  # uncomment to turn off results logging
        echo "Starting partition: $i - logging results to $OUTFILE"
        java -cp target/textanalyticsudfs-1.0.jar com.amazonaws.athena.udf.textanalytics.TextAnalyticsUDFHandler perftest $REQ_PER_PARTITION "$PAYLOAD" > $OUTFILE &
        PIDS[${i}]=$!
    done

    for pid in ${PIDS[*]}; do
        wait $pid
    done
    end=$(date +%s)
    ELAPSED_TIME=$(($end-$start))
    REQUESTS_PER_SEC=$(echo "scale=2 ; $TOTAL_REQUESTS / $ELAPSED_TIME" | bc)
    BYTES_PER_SEC=$(echo "scale=2 ; $TOTAL_REQUESTS * $PAYLOAD_BYTES / $ELAPSED_TIME" | bc)

    echo "Done all batches.. Processed $TOTAL_REQUESTS in $ELAPSED_TIME seconds."
    echo "Requests/sec: $REQUESTS_PER_SEC. Bytes/sec: $BYTES_PER_SEC"
done

