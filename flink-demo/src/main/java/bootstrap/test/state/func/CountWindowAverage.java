package bootstrap.test.state.func;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {


    private ValueState<Tuple2<Long, Long>> sum;

    @Override
    public void open(Configuration parameters) throws Exception {


        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(5))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ValueStateDescriptor<Tuple2<Long, Long>> sumDesc = new ValueStateDescriptor<>(
                "sum", TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
        }));

        sumDesc.enableTimeToLive(ttlConfig);

        this.sum = getRuntimeContext().getState(sumDesc);

    }

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
        // access the state value
        Tuple2<Long, Long> currentSum = sum.value() == null ? Tuple2.of(0L, 0L) : sum.value();

        // update the count
        currentSum.f0 += 1;

        // add the second field of the input value
        currentSum.f1 += input.f1;

        // update the state
        sum.update(currentSum);

        // if the count reaches 2, emit the average and clear the state
        if (currentSum.f0 >= 2) {
            out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
            sum.clear();
        }
    }
}
