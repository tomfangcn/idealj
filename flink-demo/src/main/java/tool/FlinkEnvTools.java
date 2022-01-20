package tool;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkEnvTools {


    public static StreamExecutionEnvironment getEnv() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }
}
