package bootstrap.test;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static tool.FlinkEnvTools.MockInput;
import static tool.FlinkEnvTools.getEnv;

public class IterateJob {


    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = getEnv(1);
        DataStreamSource<Long> initialStream = env.fromCollection(MockInput.ages());



        IterativeStream<Long> iteration = initialStream.iterate();
        DataStream<Long> iterationBody  = iteration.map(age -> {Thread.sleep(1000);return age - 1;});
        DataStream<Long> feedback = iterationBody.filter( value -> value > 0);
        iteration.closeWith(feedback);
        DataStream<Long> output = iterationBody.filter( value -> value <= 0);

        output.print();

        env.execute("Iterate job");

    }

}
