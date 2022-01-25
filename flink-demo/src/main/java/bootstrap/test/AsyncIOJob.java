package bootstrap.test;

import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static tool.FlinkEnvTools.getEnv4UI;

public class AsyncIOJob {

    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = getEnv4UI(1);


        DataStreamSource<String> stu = env.socketTextStream("localhost", 9000);

        DataStream<Tuple4<String, Integer, String, Long>> stuWithTime = stu.map(AsyncIOJob::split).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<Tuple4<String, Integer, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> event.f3 * 1000));

        AsyncDataStream
                .unorderedWait(stuWithTime, new AsyncDatabaseRequest(), 1000, TimeUnit.MILLISECONDS, 100)
                .print();

        AsyncDataStream
                .orderedWait(stuWithTime, new AsyncDatabaseRequest(), 1000, TimeUnit.MILLISECONDS, 100)
                .print();

        env.execute("AsyncIOJob");
    }


    private static class AsyncDatabaseRequest extends RichAsyncFunction<Tuple4<String, Integer, String, Long>, Tuple3<String, Integer, Long>> {
        private transient AsyncClient client;

        @Override
        public void open(Configuration parameters) throws Exception {

            client = new AsyncClient();
        }

        @Override
        public void asyncInvoke(Tuple4<String, Integer, String, Long> input, ResultFuture<Tuple3<String, Integer, Long>> resultFuture) throws Exception {
            final Future<Tuple3<String, Integer, Long>> result = client.query(input);

            CompletableFuture.supplyAsync(new Supplier<Tuple3<String, Integer, Long>>() {
                @Override
                public Tuple3<String, Integer, Long> get() {
                    try {
                        return result.get();
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            }).thenAccept((Tuple3<String, Integer, Long> upRes) -> {
                resultFuture.complete(Collections.singleton(upRes));
            });

        }
    }

    private static class AsyncClient {

        public Future<Tuple3<String, Integer, Long>> query(Tuple4<String, Integer, String, Long> input) {

            return CompletableFuture.supplyAsync(() -> Tuple3.of(input.f0, input.f1, input.f3));
        }
    }

    private static Tuple4<String, Integer, String, Long> split(String input) {
        String[] str = input.trim().split(" ");
        return Tuple4.of(str[0], Integer.valueOf(str[1]), str[2], Long.valueOf(str[3]));
    }
}
