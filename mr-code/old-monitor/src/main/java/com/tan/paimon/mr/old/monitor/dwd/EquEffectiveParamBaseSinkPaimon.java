package com.tan.paimon.mr.old.monitor.dwd;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class EquEffectiveParamBaseSinkPaimon {

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
        conf.setString("state.checkpoints.dir", "hdfs://hadoop102:8020/meiotds/chk/mkt/dwd");

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .withConfiguration(conf)
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.executeSql("CREATE CATALOG meiotds WITH (\n" +
                "  'type' = 'paimon',\n" +
                "  'metastore' = 'hive',\n" +
                "  'uri' = 'thrift://hadoop102:9083',\n" +
                "  'warehouse' = 'hdfs://hadoop102:8020/user/hive/warehouse/meiotds'\n" +
                ");");

        tableEnv.executeSql("USE CATALOG meiotds;");

        tableEnv.executeSql("CREATE TEMPORARY TABLE IF NOT EXISTS kafka_source (\n" +
                "  `startDate` STRING,\n" +
                "  `equId` STRING,\n" +
                "  `deptName` STRING,\n" +
                "  `roomNo` STRING,\n" +
                "  `bedNo` STRING,\n" +
                "  `HR` STRING,\n" +
                "  `PVCs` STRING,\n" +
                "  `RR` STRING,\n" +
                "  `SpO2` STRING,\n" +
                "  `PR` STRING,\n" +
                "  `ts` BIGINT,\n" +
                "  `ts_ltz` AS TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "  WATERMARK FOR ts_ltz AS ts_ltz\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'temp_06_14',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092,hadoop103:9092,hadoop104:9092',\n" +
                "  'properties.group.id' = 'MKT_INFUSION_PUMP_HP_SERIES_JSON_06_22',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ");");

        /**
         tableEnv.sqlQuery("select * from kafka_source")
         .execute()
         .print();
         */

        // tableEnv.executeSql("DROP TABLE dwd_mr_old_monitor_equ_effective_param_detail");
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS `default`.`dwd_mr_old_monitor_equ_effective_param_detail` (\n" +
                "  \n" +
                "  `start_date` STRING,\n" +
                "  `equ_id` STRING,\n" +
                "  `dept_name` STRING,\n" +
                "  `room_no` STRING,\n" +
                "  `bed_no` STRING,\n" +
                "  `hr` STRING,\n" +
                "  `pvcs` STRING,\n" +
                "  `rr` STRING,\n" +
                "  `spo2` STRING,\n" +
                "  `pr` STRING,\n" +
                "\n" +
                "  `ts` BIGINT,\n" +
                "  `dt` STRING,\n" +
                "\n" +
                "   PRIMARY KEY (dt, equ_id, ts) NOT ENFORCED\n" +
                ") PARTITIONED BY (dt) WITH (\n" +
                "  'bucket' = '1', -- 指定 bucket 个数\n" +
                "  'bucket-key' = 'equ_id,ts',\n" +
                "  -- 记录排序字段\n" +
                "  'sequence.field' = 'ts', \n" +
                "  -- 选择 full-compaction ，在compaction后产生完整的changelog\n" +
                "  'changelog-producer' = 'full-compaction',  \n" +
                "  -- compaction 间隔时间\n" +
                "  'changelog-producer.compaction-interval' = '2 min', \n" +
                "  'merge-engine' = 'partial-update',\n" +
                "  -- 忽略DELETE数据，避免运行报错\n" +
                "  'partial-update.ignore-delete' = 'true' \n" +
                ");");

        /**

         tableEnv.sqlQuery("SELECT\n" +
         "  `startDate` AS `start_date`,\n" +
         "  `equId` AS `equ_id`,\n" +
         "  `deptName` AS `dept_name`,\n" +
         "  `roomNo` AS `room_no`,\n" +
         "  `bedNo` AS `bed_no`,\n" +
         "  `HR` AS `hr`,\n" +
         "  `PVCs` AS `pvcs`,\n" +
         "  `RR` AS `rr`,\n" +
         "  `SpO2` AS `spo2`,\n" +
         "  `PR` AS `pr`,\n" +
         "  `ts` AS `ts`,\n" +
         "   DATE_FORMAT(`ts_ltz`,'yyyy-MM-dd') as dt \n" +
         "FROM kafka_source;")
         .execute()
         .print();
         */

        tableEnv.executeSql("INSERT INTO `default`.`dwd_mr_old_monitor_equ_effective_param_detail` SELECT\n" +
                "  `startDate` AS `start_date`,\n" +
                "  `equId` AS `equ_id`,\n" +
                "  `deptName` AS `dept_name`,\n" +
                "  `roomNo` AS `room_no`,\n" +
                "  `bedNo` AS `bed_no`,\n" +
                "  `HR` AS `hr`,\n" +
                "  `PVCs` AS `pvcs`,\n" +
                "  `RR` AS `rr`,\n" +
                "  `SpO2` AS `spo2`,\n" +
                "  `PR` AS `pr`,\n" +
                "  `ts` AS `ts`,\n" +
                "   DATE_FORMAT(`ts_ltz`,'yyyy-MM-dd') as dt\n" +
                "FROM kafka_source;");

    }

}
