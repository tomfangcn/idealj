package bootstrap.test;

import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import tool.DateTool;

import java.time.Duration;

import static tool.FlinkEnvTools.MockInput;
import static tool.FlinkEnvTools.getEnv;

public class JoinJobV2 {

    public static void main(String[] args) {
        int joinType = 3;
        switch (joinType) {
            case 0:
                // 内连接--Tumbling Window Join
                run(new JoinCall<Tuple4<String, Integer, String, Long>, Tuple4<String, Integer, String, Long>>() {
                    @Override
                    public void join(DataStream<Tuple4<String, Integer, String, Long>> first, DataStream<Tuple4<String, Integer, String, Long>> second) {
                        first.join(second).where(s -> s.f0).equalTo(l -> l.f0)
                                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                                .apply(new JoinFunction<Tuple4<String, Integer, String, Long>, Tuple4<String, Integer, String, Long>, Tuple3<String, String, String>>() {
                                    @Override
                                    public Tuple3<String, String, String> join(Tuple4<String, Integer, String, Long> first, Tuple4<String, Integer, String, Long> second) throws Exception {
                                        return Tuple3.of(first.f0, second.f2, DateTool.fmtSec1(second.f3));
                                    }
                                }).print();
                    }
                });
                break;
            case 1:
                // 内连接--Sliding Window Join
                run(new JoinCall<Tuple4<String, Integer, String, Long>, Tuple4<String, Integer, String, Long>>() {
                    @Override
                    public void join(DataStream<Tuple4<String, Integer, String, Long>> first, DataStream<Tuple4<String, Integer, String, Long>> second) {
                        first.join(second).where(s -> s.f0).equalTo(l -> l.f0)
                                .window(SlidingEventTimeWindows.of(Time.seconds(60)/* size */, Time.seconds(30)/* slide */))
                                .apply(new JoinFunction<Tuple4<String, Integer, String, Long>, Tuple4<String, Integer, String, Long>, Tuple3<String, String, String>>() {
                                    @Override
                                    public Tuple3<String, String, String> join(Tuple4<String, Integer, String, Long> first, Tuple4<String, Integer, String, Long> second) throws Exception {
                                        return Tuple3.of(first.f0, second.f2, DateTool.fmtSec1(second.f3));
                                    }
                                }).print();
                    }
                });
                break;
            case 2:

                // 内连接--Session Window Join
                run(new JoinCall<Tuple4<String, Integer, String, Long>, Tuple4<String, Integer, String, Long>>() {
                    @Override
                    public void join(DataStream<Tuple4<String, Integer, String, Long>> first, DataStream<Tuple4<String, Integer, String, Long>> second) {
                        first.join(second).where(s -> s.f0).equalTo(l -> l.f0)
                                .window(EventTimeSessionWindows.withGap(Time.seconds(30)))
                                .apply(new JoinFunction<Tuple4<String, Integer, String, Long>, Tuple4<String, Integer, String, Long>, Tuple3<String, String, String>>() {
                                    @Override
                                    public Tuple3<String, String, String> join(Tuple4<String, Integer, String, Long> first, Tuple4<String, Integer, String, Long> second) throws Exception {
                                        return Tuple3.of(first.f0, second.f2, DateTool.fmtSec1(second.f3));
                                    }
                                }).print();
                    }
                });
                break;
            case 3:
                // 内连接--Interval Join
                run(new JoinCall<Tuple4<String, Integer, String, Long>, Tuple4<String, Integer, String, Long>>() {
                    @Override
                    public void join(DataStream<Tuple4<String, Integer, String, Long>> first, DataStream<Tuple4<String, Integer, String, Long>> second) {
                        first.keyBy(s -> s.f0).intervalJoin(second.keyBy(l -> l.f0))
                                .between(Time.seconds(-60), Time.seconds(60))
                                .lowerBoundExclusive()
                                .upperBoundExclusive()
                                .process(new ProcessJoinFunction<Tuple4<String, Integer, String, Long>, Tuple4<String, Integer, String, Long>, Tuple3<String, String, String>>() {
                                    @Override
                                    public void processElement(Tuple4<String, Integer, String, Long> first, Tuple4<String, Integer, String, Long> second, Context ctx, Collector<Tuple3<String, String, String>> out) throws Exception {
                                        out.collect(Tuple3.of(first.f0, second.f2, DateTool.fmtSec1(second.f3)));
                                    }
                                }).print();
                    }
                });

                break;
            default:

        }
    }

    @SneakyThrows
    public static void run(JoinCall jc) {
        StreamExecutionEnvironment env = getEnv(1);


        DataStreamSource<Tuple4<String, Integer, String, Long>> stu = env.fromCollection(MockInput.students());
        DataStream<Tuple4<String, Integer, String, Long>> stuWithTime = stu.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Tuple4<String, Integer, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                        .withTimestampAssigner((event, timestamp) -> event.f3 * 1000));

        DataStreamSource<Tuple4<String, Integer, String, Long>> loc = env.fromCollection(MockInput.locations());
        DataStream<Tuple4<String, Integer, String, Long>> locWithTime = loc.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Tuple4<String, Integer, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                        .withTimestampAssigner((event, timestamp) -> event.f3 * 1000));

        jc.join(stuWithTime, locWithTime);

        env.execute("inner join job");
    }

    public interface JoinCall<T, S> {
        void join(DataStream<T> first, DataStream<S> second);
    }

}
