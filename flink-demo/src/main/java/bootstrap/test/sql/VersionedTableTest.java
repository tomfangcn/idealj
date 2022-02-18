package bootstrap.test.sql;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.FormatDescriptor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static tool.FlinkEnvTools.getEnv;
import static tool.FlinkEnvTools.tblEnvFromStreamEnv;

public class VersionedTableTest {

    @SneakyThrows
    public static void main(String[] args) {

        // 创建执行环境
        StreamExecutionEnvironment env = getEnv();
        StreamTableEnvironment tEnv = tblEnvFromStreamEnv(env);

        // 定义源表

        tEnv.executeSql("CREATE TABLE products (" +
                "product_id    INT," +
                "product_name  STRING," +
                "price         DECIMAL(32, 2)," +
                "update_time   AS localtimestamp," +
                "PRIMARY KEY (product_id) NOT ENFORCED," +
                "WATERMARK FOR update_time AS update_time" +
                ") WITH (" +
                " 'connector' = 'datagen'," +
                " 'rows-per-second'='5'," +
                " 'fields.product_id.min'='1'," +
                " 'fields.product_id.max'='2'," +
                " 'fields.product_name.length'='10'," +
                " 'fields.price.min'='10'," +
                " 'fields.price.max'='20'" +
                ")");


        // 操作源表
        tEnv.executeSql("desc products").print();

        tEnv.executeSql("select * from products").print();



    }

}
