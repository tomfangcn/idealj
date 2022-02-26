package bootstrap.test.source;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySplitEnumerator<MySplit extends SourceSplit, CheckpointT> implements  SplitEnumerator<MySplit,CheckpointT> {

    // DISCOVER_INTERVAL = 60_000L
    private final long DISCOVER_INTERVAL;
    private  SplitEnumeratorContext<MySplit> enumContext;

    public MySplitEnumerator(long DISCOVER_INTERVAL, SplitEnumeratorContext<MySplit> enumContext) {
        this.DISCOVER_INTERVAL = DISCOVER_INTERVAL;
        this.enumContext = enumContext;
    }

    /**
     * A method to discover the splits.
     */
    private List<MySplit> discoverSplits() {
        return new ArrayList<>();
    }


    @Override
    public void start() {
        enumContext.callAsync(this::discoverSplits,  (splits,t) -> {
            Map<Integer, List<MySplit>> assignments = new HashMap<>();
            int parallelism = enumContext.currentParallelism();
            for (MySplit split : splits) {
                int owner = split.splitId().hashCode() % parallelism;
                assignments.computeIfAbsent(owner, s -> new ArrayList<>()).add(split);
            }
            enumContext.assignSplits(new SplitsAssignment<>(assignments));
        }, 0L, DISCOVER_INTERVAL);
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {

    }

    @Override
    public void addSplitsBack(List splits, int subtaskId) {

    }

    @Override
    public void addReader(int subtaskId) {

    }

    @Override
    public CheckpointT snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
