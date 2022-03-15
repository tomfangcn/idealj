package bootstrap.test.sql;

import bootstrap.test.function.MyTimeWindow;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import tool.FlinkEnvTools.MockInput;

import static tool.FlinkEnvTools.getEnv;
import static tool.FlinkEnvTools.tblEnvFromStreamEnv;

public class MySlideWindow  {


    public void task() {

        StreamExecutionEnvironment env = getEnv();

        env.setParallelism(1);
        StreamTableEnvironment tEnv = tblEnvFromStreamEnv(env);

        tEnv.createFunction("timeWindow", MyTimeWindow.class);

        // {"scene":1,"ctime":"2022-03-14 16:19","pv":1}
        DataStream<String> jobj = MockInput.so9901JSON(env);

//        DataStream<JSONObject> jobj = socket.map(JSON::parseObject);

//        tEnv.createTemporaryView("json_src",socket);
//
//        tEnv.executeSql("select * from json_src");
        tEnv.createTemporaryView("json_src",jobj);
//        tEnv.executeSql("desc json_src");




        tEnv.executeSql("create view json_view as " +
                "select " +
                "json_value(cast(f0 as varchar), '$.scene') as scene," +
                "json_value(cast(f0 as varchar), '$.ctime') as ctime," +
                "cast(json_value(cast(f0 as varchar), '$.pv') as bigint) as pv " +
                "from json_src");

        tEnv.executeSql("select scene, slide_time, sum (pv) pv \n" +
                "from (\n" +
                "\tselect \n" +
                "\tscene, ctime, slide_time, pv \n" +
                "\tfrom json_view, lateral table (timeWindow(ctime, 15)) as t(slide_time)\n" +
                ") as s\n" +
                "where slide_time <= ctime " +
                "group by scene,slide_time").print();




        jobj.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new MySlideWindow().task();
    }
}
