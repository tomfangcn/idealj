package tool;

import bootstrap.test.Person;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Int;

import java.util.ArrayList;
import java.util.List;

import static tool.DateTool.getTimeSec;

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


        public static List<Tuple4<String, Integer, String, Long>> students() {
            ArrayList<Tuple4<String, Integer, String, Long>> students = new ArrayList<>();

            long sec = getTimeSec("2022-01-22 14:05:00");

            for (int i = 0; i < 30; i++) {
                long tmp = sec + i * 60;

                students.add(Tuple4.of("zhansan", 13, "zhansan do something" + i, tmp));
                students.add(Tuple4.of("lisi", 15, "lisi do something" + i, tmp));
                students.add(Tuple4.of("wangwu", 17, "wangwu do something" + i, tmp));
            }


            return students;
        }

        public static List<Tuple4<String, Integer, String, Long>> locations() {
            ArrayList<Tuple4<String, Integer, String, Long>> locations = new ArrayList<>();

            long sec = getTimeSec("2022-01-22 14:05:00");

            for (int i = 0; i < 30; i++) {
                long tmp = sec + i * 60;

                locations.add(Tuple4.of("zhansan", 13, "zhansan somewhere" + i, tmp));
                locations.add(Tuple4.of("lisi", 15, "lisi somewhere" + i, tmp));
                locations.add(Tuple4.of("wangwu", 17, "wangwu somewhere" + i, tmp));
            }


            return locations;
        }


    }
}
