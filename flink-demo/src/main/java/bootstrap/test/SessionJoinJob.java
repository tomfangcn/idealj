package bootstrap.test;

import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import tool.DateTool;

import java.time.Duration;

import static tool.FlinkEnvTools.MockInput;
import static tool.FlinkEnvTools.getEnv;

public class SessionJoinJob {

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = getEnv(1);


        DataStreamSource<String> stu = env.socketTextStream("localhost",9000);
        DataStreamSource<String> loc = env.socketTextStream("localhost",9001);

        DataStream<Tuple4<String, Integer, String, Long>> stuWithTime =stu.map(SessionJoinJob::split).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Tuple4<String, Integer, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((event, timestamp) -> event.f3 * 1000));

        DataStream<Tuple4<String, Integer, String, Long>> locWithTime = loc.map(SessionJoinJob::split).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Tuple4<String, Integer, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((event, timestamp) -> event.f3 * 1000));


        stuWithTime.join(locWithTime).where(s -> s.f0).equalTo(l -> l.f0)
                .window(EventTimeSessionWindows.withGap(Time.seconds(10))).allowedLateness(Time.seconds(20))
                /* EventTimeSessionWindows 按双流最低水位线发送并计算,每一个记录生成一个窗口[timestamp, timestamp+gap],
                windowstate 保留至 endofwindow + allowedLateness , 若windowstate 删除则不参与合并 */
                .apply(new JoinFunction<Tuple4<String, Integer, String, Long>, Tuple4<String, Integer, String, Long>, Tuple3<String,String,String>>() {
                    @Override
                    public Tuple3<String, String,String> join(Tuple4<String, Integer, String, Long> first, Tuple4<String, Integer, String, Long> second) throws Exception {
                        return Tuple3.of(first.f0, second.f2, DateTool.fmtSec1(second.f3));
                    }
                }).print();


        env.execute("EventTimeSessionWindows test job");
    }

    private static Tuple4<String,Integer,String,Long> split(String input){
        String[] str = input.trim().split(" ");
        return  Tuple4.of(str[0],Integer.valueOf(str[1]),str[2],Long.valueOf(str[3]));
    }

}
