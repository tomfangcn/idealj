// flink 1.14.3
//package bootstrap.test.sql;
//
//import lombok.SneakyThrows;
//import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.*;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import static org.apache.flink.table.api.Expressions.*;
//
//import static tool.FlinkEnvTools.getEnv;
//import static tool.FlinkEnvTools.tblEnvFromStreamEnv;
//
//public class FlinkTableTest2 {
//
//    @SneakyThrows
//    public static void main(String[] args) {
//
//        // 创建执行环境
//        StreamExecutionEnvironment env = getEnv();
//        StreamTableEnvironment tEnv = tblEnvFromStreamEnv(env);
//
//        // 定义源表
//
////        tEnv.registerCatalog();
////        tEnv.useCatalog("myCatalog");
////        tEnv.useDatabase("test");
//
//
//
//        tEnv.executeSql("CREATE TABLE SourceTable (" +
//                " f_sequence INT," +
//                " f_random INT," +
//                " f_random_str STRING," +
//                " ts AS localtimestamp," +
//                " WATERMARK FOR ts AS ts" +
//                ") WITH (" +
//                " 'connector' = 'datagen'," +
//                " 'rows-per-second'='5'," +
//                " 'fields.f_sequence.kind'='sequence'," +
//                " 'fields.f_sequence.start'='1'," +
//                " 'fields.f_sequence.end'='1000'," +
//                " 'fields.f_random.min'='1'," +
//                " 'fields.f_random.max'='1000'," +
//                " 'fields.f_random_str.length'='10'" +
//                ")");
//
//        tEnv.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'blackhole') LIKE SourceTable (EXCLUDING OPTIONS)");
////        tEnv.executeSql(
////                "CREATE TABLE SinkTable (" +
////                        " f_sequence INT," +
////                        " f_random INT," +
////                        " f_random_str STRING," +
////                        " ts TIMESTAMP(3)" +
////                        ") WITH (" +
////                        " 'connector' = 'blackhole'" +
////                        ")"
////        );
//
//
//        // 操作源表
//        Table tbl2 = tEnv.from("SourceTable").select($("f_sequence"),$("f_random"),$("f_random_str"));
////        tbl2.printSchema();
////        Table tbl3 = tEnv.sqlQuery("SELECT * FROM SourceTable");
////        tbl3.printSchema();
////        tbl3.execute().print();
//
////        TableResult tbl3 = tEnv.executeSql("SELECT * FROM SourceTable");
////        tbl3.print(); // 执行输出
//        tEnv.executeSql("desc SourceTable").print();
//        tEnv.executeSql("desc SinkTable").print();
//
//        TableResult tblRes = tbl2.executeInsert("SinkTable");
//
//
//
//        tblRes.print();
////        env.execute("table api or sql test");
//
////      tableEnv.createTemporaryView("projectedTable", projTable);
//
//
//    }
//
//}
