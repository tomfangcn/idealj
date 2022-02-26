package bootstrap.test.source;

import org.apache.flink.api.connector.source.SourceSplit;

public class MockSourceSplit implements SourceSplit {
    @Override
    public String splitId() {
        return null;
    }
}
