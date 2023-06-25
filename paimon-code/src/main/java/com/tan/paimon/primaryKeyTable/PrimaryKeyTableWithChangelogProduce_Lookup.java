package com.tan.paimon.primaryKeyTable;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * 关于 Paimon Primary Key Table Changelog Producer Is Lookup 说明
 * 1 会产生完整的 changelog
 * 比如比如在第一个 checkpoint 过程中 Table 输入 70
 * 那么输出如下：
 * +I 70
 * 此时在表存储目录下生成一个 changelog-xxx-1.orc 文件(对应的数据文件 data-xxx-0.orc 保存数据 70) 里面保存了当前 checkpoint 的 changelog 的数据 比如 70
 * 还会生成一个文件 data-zzz-0.orc 里面存储 70
 * <p>
 * 第二次 checkpoint 输入 80
 * 那么输出如下：
 * -U 70
 * +U 80
 * 此时在表存储目录下生成一个 changelog-yyy-1.orc 文件(对应的数据文件 data-yyy-0.orc 保存数据 80) 里面保存了当前 checkpoint 的 changelog 的数据 比如 70 80
 * 还会生成一个文件 data-www-0.orc 里面存储  80
 * 第三次 checkpoint 输入 90 100
 * 那么输出如下：
 * -U 80
 * +U 100
 * 此时在表目录生成一个 changelog-uuu-1.orc 文件 (对应的数据文件 data-nnn-0.orc 保存数据 100) 里面保存了当前 checkpoint 的 changelog 的数据 比如 80 100
 * 还会生成一个文件 data-mmm-0.orc 里面存储 100
 * <p>
 * 如果二次 Job 重启不依赖于 savepoint 那么持续查询得到的结果为全量数据 ( primary key 合并后的数据)以及后续上游表快照产生的 changelog
 */
public class PrimaryKeyTableWithChangelogProduce_Lookup {

    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "tanbs");

        Configuration conf = new Configuration();
        // basic setting from https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/config/#basic-setup
        conf.setInteger("parallelism.default", 3);
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

        tableEnv.executeSql("CREATE TEMPORARY TABLE IF NOT EXISTS kafka_source_hp_series (\n" +
                "  `factoryNum` STRING,\n" +
                "  `equType` STRING,\n" +
                "  `equNum` STRING,\n" +
                "  `workSta` STRING,\n" +
                "  `deptName` STRING,\n" +
                "  `roomNo` STRING,\n" +
                "  `bedNo` STRING,\n" +
                "  `state` STRING,\n" +
                "  `drugName` STRING,\n" +
                "  `injectMode` STRING,\n" +
                "  `presetValue` FLOAT,\n" +
                "  `speed` FLOAT,\n" +
                "  `alreadyInjectTime` INTEGER,\n" +
                "  `remainTime` INTEGER,\n" +
                "  `alreadyInjectValue` FLOAT,\n" +
                "  `residual` FLOAT,\n" +
                "  `alarm1` STRING,\n" +
                "  `alarm2` STRING,\n" +
                "  `alarm3` STRING,\n" +
                "  `alarm4` STRING,\n" +
                "  `pressureValue` FLOAT,\n" +
                "  `pressureUint` STRING,\n" +
                "  `ts` BIGINT,\n" +
                "  `ts_ltz` AS TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "  WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '1' SECOND \n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'temp_06_12',\n" +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',\n" +
                "  'properties.group.id' = 'MKT_INFUSION_PUMP_HP_SERIES_JSON_06_22',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                ");");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS ods_kafka_source_hp_series_lookup (\n" +
                "  `factory_num` STRING,\n" +
                "  `equ_type` STRING,\n" +
                "  `equ_num` STRING,\n" +
                "  `work_sta` STRING,\n" +
                "  `dept_name` STRING,\n" +
                "  `room_no` STRING,\n" +
                "  `bed_no` STRING,\n" +
                "  `state` STRING,\n" +
                "  `drug_name` STRING,\n" +
                "  `inject_mode` STRING,\n" +
                "  `preset_value` FLOAT,\n" +
                "  `speed` FLOAT,\n" +
                "  `already_inject_time` INTEGER,\n" +
                "  `remain_time` INTEGER,\n" +
                "  `already_inject_value` FLOAT,\n" +
                "  `residual` FLOAT,\n" +
                "  `alarm1` STRING,\n" +
                "  `alarm2` STRING,\n" +
                "  `alarm3` STRING,\n" +
                "  `alarm4` STRING,\n" +
                "  `pressure_value` FLOAT,\n" +
                "  `pressure_uint` STRING,\n" +
                "  `ts` BIGINT,\n" +
                "\n" +
                "  `dt` STRING,\n" +
                "  PRIMARY KEY (dt, factory_num) NOT ENFORCED\n" +
                ") PARTITIONED BY (dt) WITH (\n" +
                "  'bucket' = '3',\n" +
                "  'bucket-key' = 'factory_num',\n" +
                "  'merge-engine' = 'deduplicate',\n" +
                "  'sequence.field' = 'ts',\n" +
                "  'changelog-producer' = 'lookup'\n" +
                ");");

        tableEnv.executeSql("INSERT INTO ods_kafka_source_hp_series_lookup SELECT \n" +
                "\n" +
                "  `factoryNum` AS `factory_num`,\n" +
                "  `equType` AS `equ_type`,\n" +
                "  `equNum` AS `equ_num`,\n" +
                "  `workSta` AS `work_sta`,\n" +
                "  `deptName` AS `dept_name`,\n" +
                "  `roomNo` AS `room_no`,\n" +
                "  `bedNo` AS `bed_no`,\n" +
                "  `state` AS `state`,\n" +
                "  `drugName` AS `drug_name`,\n" +
                "  `injectMode` AS `inject_mode`,\n" +
                "  `presetValue` AS `preset_value`,\n" +
                "  `speed` AS `speed`,\n" +
                "  `alreadyInjectTime` AS `already_inject_time`,\n" +
                "  `remainTime` AS `remain_time`,\n" +
                "  `alreadyInjectValue` AS `already_inject_value`,\n" +
                "  `residual` AS `residual`,\n" +
                "  `alarm1` AS `alarm1`,\n" +
                "  `alarm2` AS `alarm2`,\n" +
                "  `alarm3` AS `alarm3`,\n" +
                "  `alarm4` AS `alarm4`,\n" +
                "  `pressureValue` AS `pressure_value`,\n" +
                "  `pressureUint` AS `pressure_uint`,\n" +
                "  `ts`,\n" +
                "   DATE_FORMAT(ts_ltz,'yyyy-MM-dd') AS `dt`\n" +
                "FROM kafka_source_hp_series;");

    }

}
