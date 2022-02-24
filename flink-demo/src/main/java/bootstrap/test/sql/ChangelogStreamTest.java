package bootstrap.test.sql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.CURRENT_ROW;

public class ChangelogStreamTest extends StreamTableWithCfgJob {

    public ChangelogStreamTest(boolean isRunDS) {
        this.isRunDS = isRunDS;
    }

    @Override
    public void task() {

        // create a changelog DataStream
        DataStream<Row> dataStream =
                env.fromElements(
                        Row.ofKind(RowKind.INSERT, "Alice", 12),
                        Row.ofKind(RowKind.INSERT, "Bob", 5),
                        Row.ofKind(RowKind.INSERT, "Bob", 50),
                        Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
//                        Row.ofKind(RowKind.INSERT, "Alice", 88),
                        Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100)
//                        Row.ofKind(RowKind.DELETE, "Alice", 20)
                );

        // interpret the DataStream as a Table
        // columnByExpression("f2", "PROCTIME()")
        // primaryKey("f0")
        Table table = tEnv.fromChangelogStream(dataStream);

//        table.groupBy($("f0"))
//                .select($("f0").as("name"),$("f1").sum().as("score"))
//                .execute().print();

        // register the table under a name and perform an aggregation
        tEnv.createTemporaryView("InputTable", table);
        
        tEnv.executeSql("SELECT * FROM InputTable")
                .print();
        tEnv.executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0")
                .print();
        Table aggTable = tEnv.sqlQuery("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0");

        tEnv.toChangelogStream(aggTable).print();

    }

    public static void main(String[] args) {
        new ChangelogStreamTest(true).runJob();
    }
}
