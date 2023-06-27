package com.tan.paimon.queryingTable;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 官方文档指南： https://paimon.apache.org/docs/0.4/how-to/querying-tables/
 */
public class QueryingTableMain {

    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "tanbs");

        Configuration conf = new Configuration();
        // basic setting from https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/config/#basic-setup
        conf.setInteger("parallelism.default", 1);
        // checkpoint setting from https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/config/#checkpointing
        conf.setLong("execution.checkpointing.interval", 60 * 1000L);
        conf.setString("execution.checkpointing.mode", "EXACTLY_ONCE");
        conf.setLong("execution.checkpointing.timeout", 60 * 1000L);
        conf.setLong("execution.checkpointing.min-pause", 60 * 1000L);
        conf.setInteger("execution.checkpointing.max-concurrent-checkpoints", 1);
        conf.setInteger("execution.checkpointing.tolerable-failed-checkpoints", 3);
        conf.setString("execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION");
        // state backend setting from https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/config/#checkpoints-and-state-backends
        conf.setString("state.backend", "hashmap");
        conf.setString("state.checkpoint-storage", "filesystem");
        conf.setString("state.checkpoints.dir", "hdfs://hadoop102:8020/meiotds/chk/mkt/dws");

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .withConfiguration(conf)
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.getConfig().set("table.exec.sink.upsert-materialize", "NONE");

        tableEnv.executeSql("CREATE CATALOG my_catalog WITH (\n" +
                "    'type' = 'paimon',\n" +
                "    'warehouse' = 'hdfs://hadoop102:8020/paimon/code'\n" +
                ");");
        tableEnv.executeSql("USE CATALOG my_catalog;");

        /**
         * 默认 scan.mode = default
         * 先读取全量数据 + 增量数据
         */
//        tableEnv.sqlQuery("select * from ods_kafka_source_hp_series_full")
//                .execute()
//                .print();

        /**
         * scan.mode = latest-full
         * 批：上一次 snapshot 之前的数据 不包含当前 snapshot 数据
         * 流：上一次 snapshot 之前的数据 以及后续 changelog
         */
//        tableEnv.sqlQuery("select * from ods_kafka_source_hp_series_full /*+ OPTIONS('scan.mode' = 'latest-full') */")
//                .execute()
//                .print();

        /**
         * scan.mode = compacted-full
         * 批：上一次 full compact 数据
         * 流：上一次 full compact 数据 以及后续 changelog
         */
//        tableEnv.sqlQuery("select * from ods_kafka_source_hp_series_full /*+ OPTIONS('scan.mode' = 'compacted-full') */")
//                .execute()
//                .print();

        /**
         * scan.mode = latest
         * 批：与 latest-full 相同
         * 流：上一次 snapshot (不包含之前的数据) 到后续 changelog (如果后续没有数据则不打印)
         */
//        tableEnv.sqlQuery("select * from ods_kafka_source_hp_series_full /*+ OPTIONS('scan.mode' = 'latest') */")
//                .execute()
//                .print();

        /**
         * 'scan.snapshot-id' = '1'
         * 批：指定 snapshot id 数据
         * 流：指定 snapshot id 数据以及后续 changelog
         */
//        tableEnv.sqlQuery("select * from ods_kafka_source_hp_series_full /*+ OPTIONS('scan.snapshot-id' = '1') */")
//                .execute()
//                .print();

        /**
         * 'scan.timestamp-millis' = '1687673827000'
         * 批：指定 snapshot id 数据
         * 流：指定 时间戳 数据以及后续 changelog
         */
//        tableEnv.sqlQuery("select * from ods_kafka_source_hp_series_full /*+ OPTIONS('scan.timestamp-millis' = '1687673827000') */")
//                .execute()
//                .print();

        tableEnv.sqlQuery("select * from ods_kafka_source_hp_series_full /*+ OPTIONS('consumer-id' = 'myid') */")
                .execute()
                .print();


    }

}
