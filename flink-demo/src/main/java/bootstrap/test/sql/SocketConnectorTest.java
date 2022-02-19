package bootstrap.test.sql;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static tool.FlinkEnvTools.getEnv;
import static tool.FlinkEnvTools.tblEnvFromStreamEnv;

public class SocketConnectorTest {

    @SneakyThrows
    public static void main(String[] args) {

        // 创建执行环境
        StreamExecutionEnvironment env = getEnv();
        StreamTableEnvironment tEnv = tblEnvFromStreamEnv(env);

        // 定义源表

//        INSERT|Alice|12
//        INSERT|Bob|5
//        DELETE|Alice|12
//        INSERT|Alice|18
        tEnv.executeSql("CREATE TABLE UserScores (name STRING, score INT)" +
                "WITH (" +
                " 'connector' = 'socket'," +
                " 'hostname' = 'localhost'," +
                " 'port' = '9999'," +
                " 'byte-delimiter' = '10'," +
                " 'format' = 'changelog-csv'," +
                " 'changelog-csv.column-delimiter' = '|'" +
                ")");


        // 操作源表
        tEnv.executeSql("desc UserScores").print();

        tEnv.executeSql("SELECT name, SUM(score) score_sum FROM UserScores GROUP BY name").print();


    }

}
