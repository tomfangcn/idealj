package bootstrap.test.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static tool.FlinkEnvTools.getEnv;
import static tool.FlinkEnvTools.tblEnvFromStreamEnv;

public abstract class StreamTableWithCfgJob {

    public StreamExecutionEnvironment env;
    public StreamTableEnvironment tEnv;

    protected boolean isRunDS = true;

    public StreamTableWithCfgJob() {
    }

    private void init() {
        // 创建执行环境
        env = getEnv();

        configEnv(env);

        tEnv = tblEnvFromStreamEnv(env);

        configTableEnv(tEnv);


    }


    public abstract void task();

    public void runJob() {

        init();

        task();

        try {
            if (isRunDS) env.execute("test");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void configEnv(StreamExecutionEnvironment env) {
    }

    protected void configTableEnv(StreamTableEnvironment env) {
    }
}
