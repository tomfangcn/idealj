package bootstrap.test.join;

import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;

import static tool.FlinkEnvTools.getEnv;


public class IntervalJoinTest {

    public static class Order {
        public String tradeNo;
        public String orderId;
        public Integer price;
        public Integer amount;
        public String userId;
        public Instant eventTime;

        public Order(String tradeNo,String orderId, Integer price, Integer amount, String userId, Long millSec) {
            this.tradeNo = tradeNo;
            this.orderId = orderId;
            this.price = price;
            this.amount = amount;
            this.userId = userId;
            this.eventTime = Instant.ofEpochMilli(millSec);
        }
    }

    public static class User {

        public String tradeNo;
        public String id;
        public String name;
        public Instant eventTime;



        public User(String tradeNo,String id, String name, Long millSec) {
            this.tradeNo = tradeNo;
            this.id = id;
            this.name = name;
            this.eventTime = Instant.ofEpochMilli(millSec);
        }
    }

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = getEnv();

        SingleOutputStreamOperator<User> usersrc = env.socketTextStream("localhost", 9000).map(r -> {
            String[] split = r.split("\\|");
            return new User(split[0], split[1], split[2], Long.valueOf(split[3]));
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<User>forBoundedOutOfOrderness(Duration.ofMillis(5))
                        .withTimestampAssigner((r, ts) -> r.eventTime.toEpochMilli())
        );

        SingleOutputStreamOperator<Order> ordersrc = env.socketTextStream("localhost", 8000).map(r -> {
            String[] split = r.split("\\|");
            return new Order(split[0], split[1], Integer.valueOf(split[2]), Integer.valueOf(split[3]), split[4], Long.valueOf(split[5]));
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.
                        <Order>forBoundedOutOfOrderness(Duration.ofMillis(5))
                        .withTimestampAssigner((r, ts) -> r.eventTime.toEpochMilli())
        );

        // usersrc 到 ordersrc 这个方向的链接,具有方向性
        usersrc.keyBy(u -> u.tradeNo).intervalJoin(ordersrc.keyBy(o -> o.tradeNo))
                .between(Time.milliseconds(0), Time.milliseconds(10))
                .process(new ProcessJoinFunction<User, Order, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public void processElement(User user, Order order, ProcessJoinFunction<User, Order, Tuple3<String, Integer, Integer>>.Context ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {

                        System.out.println(ctx.getLeftTimestamp());
                        System.out.println(ctx.getRightTimestamp());
                        System.out.println(ctx.getTimestamp());
                        System.out.println("user time :" + user.eventTime + ", order time :" + order.eventTime);

                        out.collect(Tuple3.of(user.name, order.price, order.amount));
                    }
                })
                .print();


        env.execute("IntervalJoinTest");
    }
}
