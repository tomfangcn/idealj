// flink 1.14.3
//package bootstrap.test.sql;
//
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
//import org.apache.flink.table.api.DataTypes;
//import org.apache.flink.table.api.Schema;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.TableDescriptor;
//import org.apache.flink.table.api.bridge.java.StreamStatementSet;
//
//public class AttachAsDataStreamTest extends StreamTableJob{
//    @Override
//    public void task() {
//        StreamStatementSet statementSet = tEnv.createStatementSet();
//
//        TableDescriptor sourceDesc = TableDescriptor.forConnector("datagen")
//                .option("number-of-rows", "3")
//                .schema(
//                        Schema.newBuilder()
//                                .column("myCol", DataTypes.INT())
//                                .column("myOtherCol", DataTypes.BOOLEAN())
//                                .build()
//                ).build();
//
//        TableDescriptor sinkDesc = TableDescriptor.forConnector("print").build();
//
//        Table tableFromSource  = tEnv.from(sourceDesc);
//        statementSet.addInsert(sinkDesc,tableFromSource );
//
//        DataStreamSource<Integer> dataStream = env.fromElements(1, 2, 3);
//        Table tableFromStream  = tEnv.fromDataStream(dataStream);
//        statementSet.addInsert(sinkDesc, tableFromStream);
//
//        statementSet.attachAsDataStream();
//
//        env.fromElements(4,5,6).addSink(new DiscardingSink<>());
//
//
//
//
//    }
//
//    public static void main(String[] args) {
//        new AttachAsDataStreamTest().runJob();
//    }
//}
