// flink 1.14.3
//package bootstrap.test.sql;
//
//import lombok.SneakyThrows;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.*;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//
//import static org.apache.flink.table.api.Expressions.$;
//import static tool.FlinkEnvTools.getEnv;
//import static tool.FlinkEnvTools.tblEnvFromStreamEnv;
//
//public class FlinkSinkTableTest {
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
//                " 'fields.f_sequence.end'='10'," +
//                " 'fields.f_random.min'='1'," +
//                " 'fields.f_random.max'='10'," +
//                " 'fields.f_random_str.length'='10'" +
//                ")");
//
//
//
//
//        // 操作源表
//        tEnv.executeSql("desc SourceTable").print();
//
//
//        final Schema schema = Schema.newBuilder()
//                .column("f_sequence", DataTypes.INT())
//                .column("f_random", DataTypes.INT())
//                .column("f_random_str", DataTypes.STRING())
//                .column("ts",DataTypes.TIMESTAMP(3))
//                .build();
//
//
//        tEnv.createTemporaryTable("CsvSinkTable", TableDescriptor.forConnector("filesystem")
//                        .schema(schema)
//                        .option("path", "E:\\DEV\\learning\\flink-demo\\input\\data\\tablesink")
//                        .format(FormatDescriptor.forFormat("csv")
//                                .option("field-delimiter", "|")
//                                .build())
//                        .build());
//
//        // 落地: 方式一 sql api
//        tEnv.executeSql("INSERT INTO CsvSinkTable SELECT f_sequence, f_random, f_random_str, ts FROM SourceTable");
//
//        // 方式二 table api
////        tEnv.from("SourceTable").executeInsert("CsvSinkTable");
//
//    }
//
//}
