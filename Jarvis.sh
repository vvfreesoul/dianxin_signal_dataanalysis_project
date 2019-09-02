#!/usr/bin/env bash

# 上传文件到hdfs
hadoop fs -mkdir /dianxin/real_data/oneday_user9981
hadoop fs -put ./data/user9981/part-r-00000 /dianxin/real_data/oneday_user9981/

# 单独聚类用户9981
hadoop jar ./target/dianxin_signal_dataanalysis_project-1.0-SNAPSHOT.jar cluster_algorithm.DBscan_cluster.DBscanJob hdfs://master:9000/dianxin/config_file/4g_base_station.txt hdfs://master:9000/dianxin/real_data/oneday_user9981/part-r-00000 hdfs://master:9000/dianxin/real_data/oneday_user9981_out/

# 统计聚类结果
hadoop jar ./target/dianxin_signal_dataanalysis_project-1.0-SNAPSHOT.jar postprocessing.gatherJob  hdfs://master:9000/dianxin/real_data/oneday_user9981_out/ hdfs://master:9000/dianxin/real_data/oneday_user9981_gather_out/

# 结果打包下载
mkdir shiyan/1th
hadoop fs -getmerge /dianxin/real_data/oneday_user9981_out/ ./shiyan/1th/onday_9981_DBscan.txt
hadoop fs -getmerge /dianxin/real_data/oneday_user9981_gather_out/ ./shiyan/1th/oneday_user9981_gather_out.txt
scp -r ./shiyan/1th/ apple@10.222.219.238:/Users/apple/Desktop/dianxin_signal_dataanalysis_project/shiyan/


# hadoop fs -getmerge /test/tripgatherout/ ~/xuexl/tripgatherout1.txt

#hadoop fs -getmerge /dianxin/clean_out/ ~/xuexl/clean_out.txt
#scp clean_out.txt apple@10.222.219.238:/Users/apple/Desktop/dianxin_signal_dataanalysis_project/data/from_dsm/

#hadoop fs -getmerge /dianxin/drift_out/ ~/xuexl/drift_out.txt
#scp drift_out.txt apple@10.222.219.238:/Users/apple/Desktop/dianxin_signal_dataanalysis_project/data/from_dsm/


#hadoop fs -getmerge /dianxin/real_data/DBscan_gather_out/ ~/xuexl/DBscan_cluster_out.txt
#scp DBscan_cluster_out.txt apple@10.222.219.238:/Users/apple/Desktop/dianxin_dataanalysis_postprocess/from_DSM/
#hadoop fs -getmerge /dianxin/real_data/numJob_out/ ~/xuexl/numJob_out.txt
#scp numJob_out.txt apple@10.222.219.238:/Users/apple/Desktop/dianxin_dataanalysis_postprocess/from_DSM/
#hadoop fs -getmerge /dianxin/real_data/DBscan_gather_out/ ~/xuexl/DBscan_gather_out.txt
#scp DBscan_gather_out.txt apple@10.222.219.238:/Users/apple/Desktop/dianxin_dataanalysis_postprocess/from_DSM/


