// flink 1.14.3
//package bootstrap.test.sql;
//
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.DataTypes;
//import org.apache.flink.table.api.Schema;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.planner.expressions.In;
//import org.apache.flink.types.Row;
//import org.apache.flink.types.RowKind;
//
//public class ChangelogStreamTest extends StreamTableWithCfgJob {
//
//    public ChangelogStreamTest(boolean isRunDS) {
//        this.isRunDS = isRunDS;
//    }
//
//    @Override
//    protected void configEnv(StreamExecutionEnvironment env) {
//        env.setParallelism(2);
//    }
//
//    @Override
//    public void task() {
//
//        // create a changelog DataStream
////        DataStream<Row> dataStream = fromSocket();
//        DataStream<Row> dataStream = mockData();
//
//        Table table = tEnv.fromChangelogStream(dataStream);
//
//        // 添加 计算字段 和 主键
//        // columnByExpression("f2", "PROCTIME()")
//        // primaryKey("f0")
//
//        // 测试 table api
////        table.groupBy($("f0"))
////                .select($("f0").as("name"),$("f1").sum().as("score"))
////                .execute().print();
//
//        // register the table under a name and perform an aggregation
//        tEnv.createTemporaryView("InputTable", table);
//
//        // changlog 流程变化测试
////        Table allData = tEnv.sqlQuery("select * from InputTable");
////        allData.printSchema();
////        tEnv.toChangelogStream(allData).print();
////
////        tEnv.createTemporaryView("allData", allData);
////        Table someData = tEnv.sqlQuery("select  name,  weigh from allData");
////        someData.printSchema();
////        tEnv.toChangelogStream(someData).print();
////
////        tEnv.createTemporaryView("someData" , someData);
////        Table aggSome = tEnv.sqlQuery("select name , sum(weigh) from someData group by name");
////        aggSome.printSchema();
////        tEnv.toChangelogStream(aggSome).print();
//
//        // sql 测试
//        tEnv.executeSql("SELECT * FROM InputTable")
//                .print();
//
//        tEnv.executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0")
//                .print();
//
//        Table aggTable = tEnv.sqlQuery("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0");
//
//        tEnv.toChangelogStream(aggTable).print();
//
//    }
//
//    private DataStream<Row> fromSocket() {
//        return env.socketTextStream("localhost", 9999)
//                .map(line -> Row.of(
//                        line.split(",")[0],
//                        line.split(",")[1],
//                        Integer.valueOf(line.split(",")[2])))
//                .returns(Types.ROW_NAMED(
//                        new String[]{"id", "name", "weigh"},
//                        Types.STRING, Types.STRING, Types.INT)
//                );
//    }
//
//    private DataStream<Row> mockData() {
//        return env.fromElements(
//                Row.ofKind(RowKind.INSERT, "Alice", 12),
//                Row.ofKind(RowKind.INSERT, "Bob", 5),
//                Row.ofKind(RowKind.INSERT, "Bob", 50),
//                Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
////                        Row.ofKind(RowKind.INSERT, "Alice", 88),
//                Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100)
////                        Row.ofKind(RowKind.DELETE, "Alice", 20)
//        );
//    }
//
//    public static void main(String[] args) {
//        new ChangelogStreamTest(true).runJob();
//    }
//}
