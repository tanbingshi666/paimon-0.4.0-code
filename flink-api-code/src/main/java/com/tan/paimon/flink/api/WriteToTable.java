package com.tan.paimon.flink.api;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class WriteToTable {

    static {
        System.setProperty("HADOOP_USER_NAME", "tanbs");
    }

    public static void writeTo() {
        // create environments of both APIs
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // checkpoint setting from: https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/dev/datastream/fault-tolerance/checkpointing/
        env.enableCheckpointing(60 * 1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(60 * 1000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // state backend setting from: https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/ops/state/state_backends/
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/meiotds/chk/mkt/hp/ods");

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // create paimon catalog
        tableEnv.executeSql("CREATE CATALOG paimon WITH ('type' = 'paimon', 'warehouse'='hdfs://hadoop102:8020/paimon/api/flink')");
        tableEnv.executeSql("USE CATALOG paimon");
        tableEnv.executeSql("CREATE TABLE word_count (\n" +
                "    word STRING PRIMARY KEY NOT ENFORCED,\n" +
                "    cnt BIGINT\n" +
                ")");
        tableEnv.executeSql("CREATE TEMPORARY TABLE word_table (\n" +
                "    word STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'fields.word.length' = '1'\n" +
                ")");
        // insert into paimon table from your data stream table
        tableEnv.executeSql("INSERT INTO word_count SELECT word, COUNT(*) FROM word_table GROUP BY word");


    }

    public static void main(String[] args) {

        writeTo();

    }

}
