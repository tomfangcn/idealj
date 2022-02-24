package bootstrap.test.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;

public class DeduplicateUserTest extends StreamTableWithCfgJob {


    @Override
    protected void configEnv(StreamExecutionEnvironment env) {
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    }

    @Override
    public void task() {


        DataStream<Row> userStream = env.fromElements(
                Row.of(LocalDateTime.parse("2021-08-21T13:00:00"), 1, "Alice"),
                Row.of(LocalDateTime.parse("2021-08-21T13:05:00"), 2, "Bob"),
                Row.of(LocalDateTime.parse("2021-08-21T13:10:00"), 2, "Bob")
        ).returns(Types.ROW_NAMED(
                new String[]{"ts", "uid", "name"},
                Types.LOCAL_DATE_TIME, Types.INT, Types.STRING
        ));


        DataStream<Row> orderStream = env
                .fromElements(
                        Row.of(LocalDateTime.parse("2021-08-21T13:02:00"), 1, 122),
                        Row.of(LocalDateTime.parse("2021-08-21T13:07:00"), 2, 239),
                        Row.of(LocalDateTime.parse("2021-08-21T13:11:00"), 2, 999))
                .returns(
                        Types.ROW_NAMED(
                                new String[]{"ts", "uid", "amount"},
                                Types.LOCAL_DATE_TIME, Types.INT, Types.INT));


        tEnv.createTemporaryView(
                "UserTable",
                userStream,
                Schema.newBuilder()
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .column("uid", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .watermark("ts", "ts - INTERVAL '1' SECOND")
                        .build());

        tEnv.createTemporaryView(
                "OrderTable",
                orderStream,
                Schema.newBuilder()
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .column("uid", DataTypes.INT())
                        .column("amount", DataTypes.INT())
                        .watermark("ts", "ts - INTERVAL '1' SECOND")
                        .build());

        Table joinedTable =
                tEnv.sqlQuery(
                        "SELECT U.name, O.amount " +
                                "FROM UserTable U, OrderTable O " +
                                "WHERE U.uid = O.uid AND O.ts BETWEEN U.ts AND U.ts + INTERVAL '5' MINUTES");

        DataStream<Row> joinedStream = tEnv.toDataStream(joinedTable);

        joinedStream.print();


        joinedStream.keyBy(r -> r.<String>getFieldAs("name"))
                .process(new KeyedProcessFunction<String, Row, String>() {
                    ValueState<String> seen;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        seen = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("name", String.class)
                        );
                    }

                    @Override
                    public void processElement(Row row, KeyedProcessFunction<String, Row, String>.Context ctx, Collector<String> out) throws Exception {
                        String name = row.getFieldAs("name");

                        if (seen.value() == null) {
                            seen.update(name);
                            out.collect(name);
                        }
                    }
                })
                .print();


    }

    public static void main(String[] args) {
        new DeduplicateUserTest().runJob();
    }
}
