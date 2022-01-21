package tool;

import bootstrap.test.Person;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class FlinkEnvTools {


    public static StreamExecutionEnvironment getEnv() {
        // default RuntimeExecutionMode.STREAMING
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    public static StreamExecutionEnvironment getEnv(int parallelism) {
        // default RuntimeExecutionMode.STREAMING
        return StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(parallelism);
    }

    public static StreamExecutionEnvironment getBatchEnv() {
        return StreamExecutionEnvironment.getExecutionEnvironment().setRuntimeMode(RuntimeExecutionMode.BATCH);
    }



    public static class MockInput {

        public static List<Person> persons() {
            List<Person> people = new ArrayList<Person>();
            people.add(new Person("Fred", 35));
            people.add(new Person("Wilma", 35));
            people.add(new Person("Pebbles", 2));

            return people;
        }

        public static List<Long> ages() {
            List<Long> ages = new ArrayList<Long>();
            ages.add(3L);
            ages.add(10L);
            ages.add(17L);

            return ages;
        }

    }
}
