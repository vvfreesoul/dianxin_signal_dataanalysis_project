#!/bin/bash
hadoop jar ../dianxin_signal_dataanalysis_project-1.0-SNAPSHOT.jar cluster_algorithm.regular_cluster.regularJob hdfs://master:9000/dianxin/config_file/4g_base_station.txt hdfs://master:9000/dianxin/real_data/input/new_driftout.txt hdfs://master:9000/dianxin/real_data/regular_cluster_out/
hadoop jar ../dianxin_signal_dataanalysis_project-1.0-SNAPSHOT.jar postprocessing.dsm_gatherJob hdfs://master:9000/dianxin/real_data/regular_cluster_out/ hdfs://master:9000/dianxin/real_data/DBscan_gather_out/
hadoop fs -getmerge /dianxin/real_data/DBscan_gather_out/ ~/xuexl/autoTest/python_code/from_DSM/DBscan_cluster_out.txt



