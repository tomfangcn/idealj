package bootstrap.test;

import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import tool.DateTool;

import java.time.Duration;

import static tool.FlinkEnvTools.*;

public class JoinJob {

    public static void main(String[] args) {
        int joinType = 0;
        switch (joinType) {
            case 0:
                // 内连接
                Join.innerJoin();
                break;
            case 1:
                break;
            case 2:
                break;
            case 3:
                break;
            default:

        }
    }

    public static class Join {

        @SneakyThrows
        public static void innerJoin()  {

            StreamExecutionEnvironment env = getEnv(1);


            DataStreamSource<Tuple4<String, Integer, String, Long>> stu = env.fromCollection(MockInput.students());
            DataStream<Tuple4<String, Integer, String, Long>> stuWithTime =stu.assignTimestampsAndWatermarks(
                    WatermarkStrategy
                            .<Tuple4<String, Integer, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                            .withTimestampAssigner((event, timestamp) -> event.f3 * 1000));

            DataStreamSource<Tuple4<String, Integer, String, Long>> loc = env.fromCollection(MockInput.locations());
            DataStream<Tuple4<String, Integer, String, Long>> locWithTime = loc.assignTimestampsAndWatermarks(
                    WatermarkStrategy
                            .<Tuple4<String, Integer, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                            .withTimestampAssigner((event, timestamp) -> event.f3 * 1000));


            stuWithTime.join(locWithTime).where(s -> s.f0).equalTo(l -> l.f0)
                    .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                    .apply(new JoinFunction<Tuple4<String, Integer, String, Long>, Tuple4<String, Integer, String, Long>, Tuple3<String,String,String>>() {
                        @Override
                        public Tuple3<String, String,String> join(Tuple4<String, Integer, String, Long> first, Tuple4<String, Integer, String, Long> second) throws Exception {
                            return Tuple3.of(first.f0, second.f2, DateTool.fmtSec1(second.f3));
                        }
                    }).print();


            env.execute("inner join job");
        }

    }

}
