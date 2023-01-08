sf=1
for QUERY_ID in {18..18}
do 
    export SPARK_PARAMS="\
--master yarn \
--deploy-mode client \
--name run_query_test_sf_$sf_$QUERY_ID \
--driver-cores 5 \
--driver-memory 6g \
--executor-cores 4 \
--executor-memory 8g \
--num-executors 8 \
--conf spark.default.parallelism=64"
    #./bin/bigBench cleanQuery -d bigbench_sf_10 -q $QUERY_ID -U
    ./bin/bigBench runQuery -U -d bigbench_sf_$sf -q $QUERY_ID

done
