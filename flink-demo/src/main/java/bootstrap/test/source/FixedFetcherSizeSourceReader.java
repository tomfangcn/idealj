package bootstrap.test.source;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import java.util.Map;
import java.util.function.Supplier;

public class FixedFetcherSizeSourceReader<E, T, SplitT extends SourceSplit, SplitStateT> extends SourceReaderBase<E, T, SplitT, SplitStateT> {

    public FixedFetcherSizeSourceReader(
//            FutureNotifier futureNotifier,
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            Supplier<SplitReader<E, SplitT>> splitFetcherSupplier,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(
//                futureNotifier,
                elementsQueue,
                new FixedSizeSplitFetcherManager<>(
                        config.getInteger(SourceConfig.NUM_FETCHERS),
//                        futureNotifier,
                        elementsQueue,
                        splitFetcherSupplier),
                recordEmitter,
                config,
                context);
    }

    @Override
    protected void onSplitFinished(Map<String, SplitStateT> map) {

    }

    @Override
    protected SplitStateT initializedState(SplitT splitT) {
        return null;
    }

    @Override
    protected SplitT toSplitType(String s, SplitStateT splitStateT) {
        return null;
    }

    private static class SourceConfig {

        public static final ConfigOption<Integer> NUM_FETCHERS = null;
    }
}
