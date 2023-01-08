import os
import time
import logging

def data_reset(sf):
    # clean all BigBench related data
    os.system('./bin/bigBench runBenchmark  -i CLEAN_ALL')
    # data generation and populate metastore
    os.system('./bin/bigBench runBenchmark -m 4 -f ' + str(sf) + ' -s 2 -i DATA_GENERATION,LOAD_TEST')
    logging.info(str(sf) + 'G data generated and loaded')

def spark_params_generate(sp, qid, sf):
    spark_params = "\
--master yarn \
--deploy-mode client \
--name sf_%d_query_%d_%d_%d_%d_%.2f_%d \
--driver-cores %d \
--driver-memory %dg \
--executor-cores %d \
--executor-memory %dm \
--num-executors %d \
--conf spark.default.parallelism=%d" % (sf, qid, sp[0], sp[1], sp[2], sp[3], sp[4], sp[0], sp[1], sp[2],int(sp[3] * 1024), sp[4], sp[2] * sp[4] * 2)
    return spark_params

def run_all_queries(sp, sf, db):
    start = time.time()
    for qid in range(1,31):
        run_query(sp, qid, sf, db)
    stop = time.time()
    logging.info('---> db: %s,  executor-cores: %d, executor-memory: %dg, num-executors: %d' % (db, sp[2], sp[3], sp[4]))
    logging.info('time cost: %f ms' % (stop - start))

def run_query(sp, qid, sf, db):
    q_start = time.time()
    spark_params = spark_params_generate(sp, qid, sf)
    #print spark_params
    os.environ['SPARK_PARAMS'] = spark_params  # visible in this process + all children
    os.environ['BIG_BENCH_SCALE_FACTOR']= str(sf)
    os.system("echo $SPARK_PARAMS")
    os.system('./bin/bigBench runQuery -U -d ' + db + ' -q '+ str(qid))
    q_stop = time.time()
    #print 'q%d: %f ms' % (qid, q_stop-q_start)
    logging.info('q%d: %f ms' % (qid, q_stop-q_start))



logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename='conf_test.log',
                    filemode='a') 

sp = [4, 6, 4, 8, 8]
#scale_factors = [10, 30, 100, 300]
scale_factors = [10, 30, 100]


for sf in scale_factors:
    db = "bigbench_sf_" + str(sf)
    # sp = (DC, DM, EC, EM, NE)
    sp0 = [4, 6, 4, 8, 8]
    sp1 = [4, 6, 4, 8, 12]
    sp2 = [4, 6, 4, 16, 8]
    sp3 = [4, 6, 4, 5.33, 12]    
    
    logging.info('test0')
    run_all_queries(sp0, sf, db)
    logging.info('test1')
    run_all_queries(sp1, sf, db)
    logging.info('test2')
    run_all_queries(sp2, sf, db)
    logging.info('test3')
    run_all_queries(sp3, sf, db)

