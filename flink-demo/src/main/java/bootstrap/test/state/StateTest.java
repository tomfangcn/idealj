package bootstrap.test.state;

import bootstrap.test.state.func.CountWindowAverage;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import tool.AppTest;

public class StateTest extends AppTest {


    @Override
    protected void runTask() {

        //subTask1
//        subTask1();
        subTask2();


    }

    private void subTask1() {
        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
                .keyBy(value -> value.f0)
                .flatMap(new CountWindowAverage())
                .print();
    }


    private void subTask2() {
        env.socketTextStream("localhost", 9000)
                .map(new MapFunction<String, Tuple2<Long,Long>>() {
                    @Override
                    public Tuple2<Long, Long> map(String line) throws Exception {
                        return Tuple2.of(Long.valueOf(line.split(",")[0]), Long.valueOf(line.split(",")[1]));
                    }
                })
                .keyBy(value -> value.f0)
                .flatMap(new CountWindowAverage())
                .print();
    }

    @SneakyThrows
    public static void main(String[] args) {

        new StateTest().runJob();
    }
}
