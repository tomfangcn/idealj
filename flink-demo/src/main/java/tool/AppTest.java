package tool;

import bootstrap.test.state.StateTest;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static tool.FlinkEnvTools.getEnv;

public abstract class AppTest {

    protected StreamExecutionEnvironment env = getEnv();

    protected abstract void runTask();

    public void runJob() {

        init();

        runTask();

        try {
            this.env.execute(this.getClass().getName());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close();
        }
    }

    private void init() {
        System.out.println("start to init config, 重写 请考虑 父类 初始化 内容");
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5);

    }

    private void close() {
        System.out.println("start to close resource");

    }



}
