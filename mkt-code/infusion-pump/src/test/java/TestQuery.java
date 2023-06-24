import com.tan.paimon.mkt.infusion.pump.hp.dws.EquEffectiveParamLookupJoinInpatientInfoSinkPaimon;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class TestQuery {

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

        tableEnv.createTemporarySystemFunction("CAL_LOCATION", EquEffectiveParamLookupJoinInpatientInfoSinkPaimon.CalLocation.class);

        tableEnv.executeSql("CREATE CATALOG meiotds WITH (\n" +
                "  'type' = 'paimon',\n" +
                "  'metastore' = 'hive',\n" +
                "  'uri' = 'thrift://hadoop102:9083',\n" +
                "  'warehouse' = 'hdfs://hadoop102:8020/user/hive/warehouse/meiotds'\n" +
                ");");

        tableEnv.executeSql("USE CATALOG meiotds;");

        tableEnv.sqlQuery("select * from `equ_mkt_infusion_pump`.`dws_mkt_infusion_pump_equ_effective_param_detail_with_inpatient_info`;")
                .execute()
                .print();

    }

}
