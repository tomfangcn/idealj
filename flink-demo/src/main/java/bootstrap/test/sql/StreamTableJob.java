package bootstrap.test.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static tool.FlinkEnvTools.getEnv;
import static tool.FlinkEnvTools.tblEnvFromStreamEnv;

public abstract class StreamTableJob {

    public final StreamExecutionEnvironment env;
    public final StreamTableEnvironment tEnv;

    public StreamTableJob() {
        // 创建执行环境
        env = getEnv();
        tEnv = tblEnvFromStreamEnv(env);

    }

    public abstract void task();

    public void runJob() {

        task();

        try {
            env.execute("test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
