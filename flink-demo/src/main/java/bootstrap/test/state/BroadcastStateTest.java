package bootstrap.test.state;


import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import tool.AppTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static tool.FlinkEnvTools.*;

public class BroadcastStateTest extends AppTest {

    private final static String BROADCAST_NAME = "RulesBroadcastState";
    private final static String ITEM_NAME = "RulesBroadcastState";

    @Override
    protected void init() {
//        env.setParallelism(2);
        env.disableOperatorChaining();
    }

    @Override
    protected void runTask() {


        DataStream<Rule> ruleStream = MockInput.socket9999(env, ",").map(r -> new Rule(r.getFieldAs(0), r.getFieldAs(1), r.getFieldAs(2)));
        DataStream<Item> itemStream = MockInput.socket9998(env, ",").map(r -> new Item(r.getFieldAs(0), r.getFieldAs(1)));

        final MapStateDescriptor<String, Rule> rulesDesc = keyStringMapStateDesc(BROADCAST_NAME, Rule.class);

//        ruleStream.print();
        BroadcastStream<Rule> ruleBroadcastStream = ruleStream.broadcast(rulesDesc);


        KeyedStream<Item, String> colorPartitionedStream = itemStream.keyBy(item -> item.color);

        DataStream<String> output = colorPartitionedStream.connect(ruleBroadcastStream)
                .process(new KeyedBroadcastProcessFunction<String, Item, Rule, String>() {


                    private MapState<String, List<Item>> state;


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        state = getRuntimeContext().getMapState(strListMapStateDesc(ITEM_NAME, Item.class));
                    }

                    @Override
                    public void processElement(Item value, KeyedBroadcastProcessFunction<String, Item, Rule, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {

                        System.out.println("processElement Thread:" + Thread.currentThread().getName());

                        final String shape = value.shape;

                        Iterable<Map.Entry<String, Rule>> entries = ctx.getBroadcastState(rulesDesc).immutableEntries();

                        for (Map.Entry<String, Rule> entry : entries) {
                            String ruleName = entry.getKey();
                            Rule rule = entry.getValue();

                            List<Item> buf = state.get(ruleName);

                            if (buf == null) buf = new ArrayList<>();

                            if (shape.equalsIgnoreCase(rule.second) && buf.size() > 0) {
                                for (Item item : buf) {
                                    out.collect("MATCH: " + item.shape + "--" + shape);
                                }
                                buf.clear();
                            }


                            if (shape.equalsIgnoreCase(rule.first)) {
                                buf.clear();
                                buf.add(value);
                            }

                            if (buf.size() > 0) {
                                state.put(ruleName, buf);
                            } else {
                                state.remove(ruleName);
                            }

                        }


                    }

                    @Override
                    public void processBroadcastElement(Rule rule, KeyedBroadcastProcessFunction<String, Item, Rule, String>.Context ctx, Collector<String> out) throws Exception {

                        System.out.println("processBroadcastElement Thread:" + Thread.currentThread().getName());
                        System.out.println("processBroadcastElement time:" + System.currentTimeMillis());
                        System.out.println("get rule.name:" + rule.name);
//                        if (rule.name.equalsIgnoreCase("rule1") ) Thread.sleep(10000);
                        if (Thread.currentThread().getName().equalsIgnoreCase("Co-Process-Broadcast-Keyed (3/4)#0")) Thread.sleep(10000);

                        ctx.getBroadcastState(rulesDesc).put(rule.name, rule);

                        for (Map.Entry<String, Rule> immutableEntry : ctx.getBroadcastState(rulesDesc).immutableEntries()) {
                            System.out.println(Thread.currentThread().getName()+"-"+immutableEntry.getKey()+":"+immutableEntry.getValue());
                        }


                    }
                });
        output.print();


    }


    public static void main(String[] args) {
        new BroadcastStateTest().runJob();
    }

    private static class Rule {
        String name;
        String first;
        String second;

        public Rule(String name, String first, String second) {
            this.name = name;
            this.first = first;
            this.second = second;
        }

        @Override
        public String toString() {
            return "Rule{" +
                    "name='" + name + '\'' +
                    ", first='" + first + '\'' +
                    ", second='" + second + '\'' +
                    '}';
        }
    }

    private static class Item {
        String shape;
        String color;

        public Item(String shape, String color) {
            this.shape = shape;
            this.color = color;
        }

        @Override
        public String toString() {
            return "Item{" +
                    "shape='" + shape + '\'' +
                    ", color='" + color + '\'' +
                    '}';
        }
    }
}
