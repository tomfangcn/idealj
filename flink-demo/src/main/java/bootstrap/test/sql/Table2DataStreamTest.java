package bootstrap.test.sql;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.row;

public class Table2DataStreamTest extends StreamTableJob {


    public void task() {

        Table table = tEnv.fromValues(DataTypes.ROW(
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("age", DataTypes.INT())),
                row("john", 35),
                row("sarah", 32)
        );

        // Row.class 测试
        DataStream<Row> dsRow = tEnv.toDataStream(table, Row.class);
        dsRow.print();

        // TupleTypeInfo test
        TupleTypeInfo<Tuple2<String, Integer>> tupleType  = new TupleTypeInfo<>(Types.STRING, Types.INT);

        DataStream<Tuple2<String, Integer>> dsTuple = tEnv.toAppendStream(table, tupleType);

        dsTuple.print();

        DataStream<Tuple2<Boolean, Row>> retractStream  = tEnv.toRetractStream(table, Row.class);

        retractStream.print();


    }


    public static void main(String[] args) {


        new Table2DataStreamTest().runJob();
    }
}
