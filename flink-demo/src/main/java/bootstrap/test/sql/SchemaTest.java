package bootstrap.test.sql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.io.PrintStream;
import java.time.Duration;
import java.time.Instant;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class SchemaTest extends StreamTableWithCfgJob {

    @Override
    protected void configEnv(StreamExecutionEnvironment env) {
//        env.getConfig().setAutoWatermarkInterval(0);

    }

    @Override
    public void task() {


        // create a DataStream
        DataStream<User> dataStream =
                env.fromElements(
                                new User("Alice", 4, Instant.ofEpochMilli(1000)),
                                new User("Bob", 6, Instant.ofEpochMilli(1001)),
                                new User("Alice", 10, Instant.ofEpochMilli(1002)))
                        .assignTimestampsAndWatermarks(WatermarkStrategy
                                .<User>forBoundedOutOfOrderness(Duration.ofMillis(1))
                                .withTimestampAssigner((r, ts) -> r.event_time.toEpochMilli())
                        );


        Table table1 = tEnv.fromDataStream(dataStream);
        table1.printSchema();


        Table table2 = tEnv.fromDataStream(
                dataStream,
                Schema.newBuilder()
                        .columnByExpression("proc_time", "PROCTIME()")
                        .build());
        table2.printSchema();


        Table table3 =
                tEnv.fromDataStream(
                        dataStream,
                        Schema.newBuilder()
                                .columnByExpression("rowtime", "CAST(event_time AS TIMESTAMP_LTZ(3))")
                                .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
                                .build());
        table3.printSchema();


        Table table4 =
                tEnv.fromDataStream(
                        dataStream,
                        Schema.newBuilder()
                                .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
                                .watermark("rowtime", "SOURCE_WATERMARK()")
                                .build());
        table4.printSchema();

        table4.execute().print();


        tEnv.executeSql(
                "CREATE TABLE GeneratedTable "
                        + "("
                        + "  name STRING,"
                        + "  score INT,"
                        + "  start_time TIMESTAMP_LTZ(3),"
                        + "  event_time TIMESTAMP_LTZ(3),"
                        + "  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND"
                        + ")"
                        + "WITH ('connector'='datagen')");

        Table table5 = tEnv.from("GeneratedTable")
                .select($("name"),$("score"),lit(Instant.ofEpochMilli(1645711874907L)).as("start_time"),$("event_time"));

//        tEnv.toDataStream(table5).process(new ProcessFunction<Row, String>() {
//            @Override
//            public void processElement(Row value, ProcessFunction<Row, String>.Context ctx, Collector<String> out) throws Exception {
//                System.out.println(value.getFieldNames(true));
//                System.out.println(value.<Instant>getFieldAs("event_time").toEpochMilli() == ctx.timestamp());
//                out.collect("");
//            }
//        }).addSink(new DiscardingSink<>());

        table5.printSchema();
        DataStream<Row> dataStream5 = tEnv.toChangelogStream(
                table5,
                Schema.newBuilder()
                        // base-position, 最好 使用full 字段 , 对每个字段定制化处理 , table <-> datastream
                        // ctx.timestamp() 是 mapping columnByMetadata 对应位置的列值
                        .column("name","STRING")
                        .column("score","INT")
                        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)") // replace feel
                        .column("event_time","TIMESTAMP_LTZ(3)")
                        .build());


        dataStream5.process(
                new ProcessFunction<Row, Void>() {
                    @Override
                    public void processElement(Row row, Context ctx, Collector<Void> out) {

                        // prints: [name, score]
                        System.out.println(row.getFieldNames(true));

                        // timestamp exists once
                        System.out.println(ctx.timestamp());
                        System.out.println(row.<Instant>getFieldAs("event_time").toEpochMilli());
                        System.out.println(row.<String>getFieldAs("name"));
                        System.out.println(row.<Integer>getFieldAs("score"));
                        //  System.out.println(row.<Integer>getFieldAs("rowtime")); // 没有此字段
                        // System.out.println(row.<Instant>getFieldAs("event_time").toEpochMilli());
                    }
                });

    }

    public static void main(String[] args) {
        new SchemaTest().runJob();
    }

    // some example POJO
    public static class User {
        public String name;

        public Integer score;

        public Instant event_time;

        // default constructor for DataStream API
        public User() {
        }

        // fully assigning constructor for Table API
        public User(String name, Integer score, Instant event_time) {
            this.name = name;
            this.score = score;
            this.event_time = event_time;
        }
    }

}

