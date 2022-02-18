package bootstrap.test.sql;

import lombok.SneakyThrows;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static tool.FlinkEnvTools.*;
import static org.apache.flink.table.api.Expressions.*;

public class FlinkTableTest {

    @SneakyThrows
    public static void main(String[] args) {

        // 创建执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
//        StreamExecutionEnvironment env = getEnv();
//        StreamTableEnvironment tEnv = tblEnvFromStreamEnv(env);

        // 定义源表
        TableDescriptor td = TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .build())
                .option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
                .build();


        tEnv.createTemporaryTable("SourceTable", td);


//        tEnv.useCatalog("myCatalog");
//        tEnv.useDatabase("test");

//        tEnv.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'blackhole') LIKE SourceTable");


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

        // 操作源表
//        Table tbl2 = tEnv.from("SourceTable");

        Table tbl3 = tEnv.sqlQuery("SELECT * FROM SourceTable");

//        tbl3.printSchema();

        // 执行输出
        tbl3.execute().print();

//        TableResult tblRes = tbl2.executeInsert("SinkTable");


//        env.execute("table api or sql test");

        System.out.println("job finish");


    }


}
