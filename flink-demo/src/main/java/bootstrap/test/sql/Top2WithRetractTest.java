package bootstrap.test.sql;

import bootstrap.test.function.Top2WithRetract;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;
import static tool.FlinkEnvTools.getEnv;
import static tool.FlinkEnvTools.tblEnvFromStreamEnv;

public class Top2WithRetractTest {

    public static void main(String[] args) {
        // 创建执行环境
        StreamExecutionEnvironment env = getEnv();


        StreamTableEnvironment tEnv = tblEnvFromStreamEnv(env);


//        INSERT|1|milk|6
//        INSERT|2|orange|7
//        INSERT|3|tea|5
//        INSERT|4|coco|3
        tEnv.executeSql("CREATE TABLE beverage (id INT PRIMARY KEY,name STRING, price INT)" +
                "WITH (" +
                " 'connector' = 'socket'," +
                " 'hostname' = 'localhost'," +
                " 'port' = '9999'," +
                " 'byte-delimiter' = '10'," +
                " 'format' = 'retract-csv'," +
                " 'retract-csv.column-delimiter' = '|'" +
                ")");


        Table tbl1 = tEnv.from("beverage");

        tEnv.createTemporarySystemFunction("top2", Top2WithRetract.class);

        Table result = tbl1.groupBy($("id"))
                .flatAggregate(call("top2", $("price")).as("price", "rank"))
                .select($("id"), $("price"), $("rank"));

        result.execute().print();
    }
}
