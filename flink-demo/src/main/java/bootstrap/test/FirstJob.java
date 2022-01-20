package bootstrap.test;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static tool.FlinkEnvTools.*;

public class FirstJob {



    public static void main(String[] args) throws Exception{



        final StreamExecutionEnvironment env = getEnv();

        DataStream<Person> input = env.fromElements(
                new Person("ZHANSAN",10),
                new Person("lisi",9),
                new Person("wangwu", 1)
        );

        DataStream<Person> filter = input.filter(p -> p.age >= 9);

        filter.print();

        env.execute("first job");

    }


    public static class Person {
        public String name;
        public Integer age;
        public Person() {}

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        public String toString() {
            return this.name.toString() + ": age " + this.age.toString();
        }
    }
}
