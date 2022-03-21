package bootstrap.test.connector.kafka;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static tool.FlinkEnvTools.getEnv;

public class KafkaCnt {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = getEnv();

        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.executeSql("create table kafka_table_source ( " +
                "`user_id` BIGINT," +
                "`item_id` BIGINT," +
                "`behavior` STRING," +
                "`ts` TIMESTAMP(3) METADATA FROM 'timestamp'" +
                " ) WITH ( " +
                "'connector'='kafka'," +
                "'properties.bootstrap.servers'='192.168.0.113:9092'," +
                "'topic'='test'," +
                "'properties.group.id'='KafkaCnt01'," +
                "'scan.startup.mode'='latest-offset'," +
                "'format'='json'," +
                "'json.ignore-parse-errors'='true'" +
                " )");


        tenv.executeSql("create table kafka_table_sink ( " +
                "`user_id` BIGINT," +
                "`item_id` BIGINT" +
                " ) WITH ( " +
                "'connector'='kafka'," +
                "'properties.bootstrap.servers'='192.168.0.113:9092'," +
                "'topic'='store'," +
                "'format'='json'" +
                " )");

        tenv.executeSql("create table kafka_01_table_sink ( " +
                "`user_id` BIGINT," +
                "`item_id` BIGINT" +
                " ) WITH ( " +
                "'connector'='kafka'," +
                "'properties.bootstrap.servers'='192.168.0.113:9092'," +
                "'topic'='order'," +
                "'format'='csv'" +
                " )");

//        tenv.createStatementSet()
//                .addInsertSql("insert into kafka_01_table_sink select user_id,item_id from kafka_table_source")
//                .addInsertSql("insert into kafka_table_sink select user_id,item_id from  kafka_table_source")
//                .execute();

        tenv.executeSql("insert into kafka_01_table_sink select user_id,item_id from  kafka_table_source");
        tenv.executeSql("insert into kafka_table_sink select user_id,item_id from  kafka_table_source");

    }

}
