package bootstrap.test.source;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class FixedSizeSplitFetcherManager<E, SplitT extends SourceSplit> extends SplitFetcherManager<E, SplitT> {

    private final int numFetchers;

    public FixedSizeSplitFetcherManager(
            int numFetchers,
            // FutureNotifier futureNotifier, 识别不了这个示例字段, super 也没有这个字段, 是否是api 已经改变?
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            Supplier<SplitReader<E, SplitT>> splitReaderSupplier) {
        super(elementsQueue, splitReaderSupplier);
        this.numFetchers = numFetchers;
        // Create numFetchers split fetchers.
        for (int i = 0; i < numFetchers; i++) {
            startFetcher(createSplitFetcher());
        }
    }

    @Override
    public void addSplits(List<SplitT> splitsToAdd) {
        // Group splits by their owner fetchers.
        Map<Integer, List<SplitT>> splitsByFetcherIndex = new HashMap<>();
        splitsToAdd.forEach(split -> {

            // 如果hash冲突,则同一个 fetcher 可能消费两个 split
            int ownerFetcherIndex = split.hashCode() % numFetchers;
            splitsByFetcherIndex
                    .computeIfAbsent(ownerFetcherIndex, s -> new ArrayList<>())
                    .add(split);
        });
        // Assign the splits to their owner fetcher.
        splitsByFetcherIndex.forEach((fetcherIndex, splitsForFetcher) -> {
            fetchers.get(fetcherIndex).addSplits(splitsForFetcher);
        });
    }
}
