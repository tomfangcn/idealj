package bootstrap.test.connector.kafka;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static tool.FlinkEnvTools.getEnv;

public class DIMJoinFromKafkaTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = getEnv();


        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        tenv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        tenv.executeSql("create table kafka_table_source ( " +
                "`user_id` BIGINT," +
                "`item_id` BIGINT," +
                "`behavior` STRING," +
                "`ts` TIMESTAMP(3)" +
//                "`ts` TIMESTAMP(3) METADATA FROM 'timestamp'" +
//                "WATERMARK FOR ts AS ts" +
                " ) WITH ( " +
                "'connector'='kafka'," +
                "'properties.bootstrap.servers'='192.168.0.113:9092'," +
                "'topic'='test'," +
                "'properties.group.id'='KafkaCnt01'," +
                "'scan.startup.mode'='latest-offset'," +
                "'format'='json'," +
                "'json.ignore-parse-errors'='true'" +
                " )");


        tenv.executeSql("create table kafka_dim_source ( " +
                "`user_id` BIGINT," +
                "`name` STRING," +
                "`ts` TIMESTAMP(3)" +
//                "`ts` TIMESTAMP(3) METADATA FROM 'timestamp'" +
//                "WATERMARK FOR ts AS ts" +
                " ) WITH ( " +
                "'connector'='kafka'," +
                "'properties.bootstrap.servers'='192.168.0.113:9092'," +
                "'topic'='dim'," +
                "'properties.group.id'='KafkaCnt01'," +
                "'scan.startup.mode'='latest-offset'," +
                "'format'='json'," +
                "'json.ignore-parse-errors'='true'" +
                " )");


//        tenv.executeSql("create view join_view as select s.user_id,t.name,s.item_id,s.behavior from " +
//                "kafka_table_source as s full outer join kafka_dim_source as t " +
//                "on s.user_id=t.user_id");


        tenv.executeSql("create view join_view as " +
                "select s.user_id,t.name,s.item_id,s.behavior,s.ts sts,t.ts tts from " +
                "kafka_table_source  s , kafka_dim_source  t " +
                "where s.user_id=t.user_id " +
                "and s.ts between t.ts - interval '10' second " +
                "and t.ts +  interval '10' second");


        tenv.executeSql("select * from join_view").print();

//        tenv.executeSql("create table kafka_table_sink ( " +
//                "`user_id` BIGINT," +
//                "`item_id` BIGINT" +
//                " ) WITH ( " +
//                "'connector'='kafka'," +
//                "'properties.bootstrap.servers'='192.168.0.113:9092'," +
//                "'topic'='store'," +
//                "'format'='json'" +
//                " )");

//        tenv.executeSql("create table kafka_01_table_sink ( " +
//                "`user_id` BIGINT," +
//                "`item_id` BIGINT" +
//                " ) WITH ( " +
//                "'connector'='kafka'," +
//                "'properties.bootstrap.servers'='192.168.0.113:9092'," +
//                "'topic'='order'," +
//                "'format'='csv'" +
//                " )");

//        tenv.createStatementSet()
//                .addInsertSql("insert into kafka_01_table_sink select user_id,item_id from kafka_table_source")
//                .addInsertSql("insert into kafka_table_sink select user_id,item_id from  kafka_table_source")
//                .execute();

//        tenv.executeSql("insert into kafka_01_table_sink select user_id,item_id from  kafka_table_source");
//        tenv.executeSql("insert into kafka_table_sink select user_id,item_id from  kafka_table_source");

    }

}
