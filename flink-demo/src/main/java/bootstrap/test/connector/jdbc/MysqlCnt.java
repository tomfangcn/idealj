package bootstrap.test.connector.jdbc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static tool.FlinkEnvTools.getEnv;

public class MysqlCnt {

    private MysqlCnt() {

    }

    static StreamExecutionEnvironment env = getEnv(1);

    static StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

    private static TableResult sql(String sql) {
        return tenv.executeSql(sql);
    }

    public static void main(String[] args) {


        sql("create table mysql_source ( " +
                "`name` STRING," +
                "`id` int," +
                "`password` STRING," +
                "`uid` int," +
                "`ts` TIMESTAMP(3)," +
                "WATERMARK FOR ts as ts," +
                "PRIMARY KEY (uid) NOT ENFORCED" +
                " ) WITH ( " +
                "'connector'='mysql-cdc'," +
                "'hostname'='localhost'," +
                "'port'='3306'," +
                "'table-name'='user'," +
                "'database-name'='test'," +
                "'username'='root'," +
                "'password'='123456'" +
                " )");


//        sql("create table mysql_source ( " +
//                "`name` STRING," +
//                "`id` int," +
//                "`password` STRING," +
//                "`uid` int," +
//                "`ts` TIMESTAMP(3)," +
//                "WATERMARK FOR ts as ts," +
//                "PRIMARY KEY (uid) NOT ENFORCED" +
//                " ) WITH ( " +
//                "'connector'='jdbc'," +
//                "'url'='jdbc:mysql://localhost:3306/test'," +
//                "'table-name'='user'," +
//                "'username'='root'," +
//                "'password'='123456'" +
//                " )");

//        tenv.executeSql("create view mysql_view as select uid,name from mysql_source group by uid,name");

        sql("create table kafka_table_source ( " +
                "`user_id` int," +
                "`item_id` BIGINT," +
                "`behavior` STRING," +
                "`ts` TIMESTAMP(3)," +
                "WATERMARK FOR ts AS ts" +
                " ) WITH ( " +
                "'connector'='kafka'," +
                "'properties.bootstrap.servers'='192.168.0.113:9092'," +
                "'topic'='test'," +
                "'properties.group.id'='KafkaCnt01'," +
                "'scan.startup.mode'='latest-offset'," +
                "'format'='json'," +
                "'json.ignore-parse-errors'='true'" +
                " )");


//        sql("select * from mysql_source").print();
        sql("select s.*,t.* from  kafka_table_source s left join mysql_source  " +
                "FOR SYSTEM_TIME AS OF s.ts as t " +
                "ON s.user_id = t.uid").print();




    }

}
