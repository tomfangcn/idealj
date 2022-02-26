package bootstrap.test.state.func;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class CounterSource extends RichParallelSourceFunction<Long> implements CheckpointedFunction {

    /**  current offset for exactly once semantics */
    private Long offset = 0L;

    /** flag for job cancellation */
    private volatile boolean isRunning = true;

    /** Our state object. */
    private ListState<Long> state;

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

        state.clear();
        state.add(offset);

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        state = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
                "state",
                LongSerializer.INSTANCE));


        // 因为状态小,所以 不用判断 isRestored,直接恢复,待验证
        for (Long l : state.get()) {
            offset = l;
        }

    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {

        final Object lock = ctx.getCheckpointLock();

        while (isRunning) {
            // output and state update are atomic
            synchronized (lock) {
                ctx.collect(offset);
                offset += 1;
            }
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
