package bootstrap.test;

import lombok.SneakyThrows;
import lombok.ToString;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static tool.FlinkEnvTools.getEnv4UI;

public class KeyedProcessJob {

    public static final Logger log = LoggerFactory.getLogger(KeyedProcessJob.class);

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = getEnv4UI(1);


        DataStreamSource<String> stu = env.socketTextStream("localhost", 9000);

        DataStream<Tuple4<String, Integer, String, Long>> stuWithTime = stu.map(KeyedProcessJob::split).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Tuple4<String, Integer, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> event.f3 * 1000));

        stuWithTime.keyBy(s->s.f0).process(new CountWithTimeoutFunction()).print();


        env.execute("KeyedProcessJob test job");
    }

    private static Tuple4<String, Integer, String, Long> split(String input) {
        String[] str = input.trim().split(" ");
        return Tuple4.of(str[0], Integer.valueOf(str[1]), str[2], Long.valueOf(str[3]));
    }


    @ToString
    private static class CountWithTimestamp {
        public String key;
        public long count;
        public long lastModified;
    }

    private static class CountWithTimeoutFunction extends KeyedProcessFunction<String, Tuple4<String, Integer, String, Long>, Tuple3<String, Long, Long>> {

        private static final int TRIGGERTIME = 60000;

        private ValueState<CountWithTimestamp> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple4<String, Integer, String, Long>, Tuple3<String, Long, Long>>.OnTimerContext ctx, Collector<Tuple3<String, Long, Long>> out) throws Exception {
            CountWithTimestamp result = state.value();
            log.info("onTimer timestamp: {} and triggerTime: {}",timestamp,result.lastModified + TRIGGERTIME);
            if (timestamp == result.lastModified + TRIGGERTIME) {
                out.collect(Tuple3.of(result.key, result.count, result.lastModified));
            }

        }

        @Override
        public void processElement(Tuple4<String, Integer, String, Long> value, KeyedProcessFunction<String, Tuple4<String, Integer, String, Long>, Tuple3<String, Long, Long>>.Context ctx, Collector<Tuple3<String, Long, Long>> out) throws Exception {


            CountWithTimestamp current = state.value();
            log.info("last current {}",current);
            if (current == null) {
                current = new CountWithTimestamp();
                current.key = value.f0;
            }

            current.count++;
            current.lastModified = ctx.timestamp();


            state.update(current);

            log.warn("timestamp():{} ,\ntimerService().currentWatermark(): {},\ntimerService().currentProcessingTime(): {}"
                    ,ctx.timestamp(),ctx.timerService().currentWatermark(),ctx.timerService().currentProcessingTime());
            ctx.timerService().registerEventTimeTimer(current.lastModified + TRIGGERTIME);
        }


    }
}
