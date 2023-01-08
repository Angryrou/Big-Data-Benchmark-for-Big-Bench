#./bin/bigBench runBenchmark  -i CLEAN_ALL

export BIG_BENCH_HOME=~/chenghao/Big-Data-Benchmark-for-Big-Bench
export PATH=${BIG_BENCH_HOME}/bin/:${PATH}

for sf in 100
do 
    export SPARK_PARAMS="\
--master yarn \
--deploy-mode client \
--name populate_metastore_$sf \
--driver-cores 5 \
--driver-memory 60g \
--executor-cores 4 \
--executor-memory 90g \
--num-executors 12 \
--conf spark.default.parallelism=64"
    export BIG_BENCH_DEFAULT_DATABASE="bigbench_sf_$sf"
    ./bin/bigBench cleanData -U
    ./bin/bigBench dataGen -U -m 288 -f $sf
    ./bin/bigBench populateMetastore -U -d bigbench_sf_${sf}
done

#export BIG_BENCH_DEFAULT_DATABASE="bigbench_sf_$sf"
#./bin/bigBench runBenchmark -m 4 -f $sf -s 2 -i DATA_GENERATION
#./bin/bigBench runBenchmark -m 4 -f $sf -s 2 -i LOAD_TEST
#./bin/bigBench cleanData -U 
#./bin/bigBench dataGen -U -m 222 -f $sf
#./bin/bigBench populateMetastore -U -d bigbench_sf_${sf}
