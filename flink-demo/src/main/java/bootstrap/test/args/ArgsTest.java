package bootstrap.test.args;

import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.io.FileInputStream;
import java.util.Map;
import java.util.Properties;

import static tool.FlinkEnvTools.getEnv;

public class ArgsTest {

    @SneakyThrows
    public static void main(String[] args) {


        StreamExecutionEnvironment env = getEnv();

        // From .properties files

        String propertiesFilePath = "/home/sam/flink/myjob.properties";
        ParameterTool.fromPropertiesFile(propertiesFilePath);

        ParameterTool.fromPropertiesFile(new File(propertiesFilePath));

        ParameterTool.fromPropertiesFile(new FileInputStream(new File(propertiesFilePath)));

        // From the command line arguments

        // --input /a/b/c.txt --output /a/b.txt
        ParameterTool fromArgs = ParameterTool.fromArgs(args);


        // From system properties

        // -Dinput=1.txt
        ParameterTool fromSystemProperties = ParameterTool.fromSystemProperties();

        fromSystemProperties.getRequired("user");

        fromSystemProperties.get("name", "tom");

        fromSystemProperties.getNumberOfParameters();

        // 相同key 会覆盖
        fromSystemProperties.mergeWith(fromArgs);

        fromSystemProperties.get("age");

        fromSystemProperties.getLong("offset", 0);

        fromSystemProperties.getLong("length");

        fromSystemProperties.getBoolean("shutdown");
        fromSystemProperties.getBoolean("start", false);

        fromSystemProperties.has("hdfs");

        Configuration configuration = fromSystemProperties.getConfiguration();

        Properties properties = fromSystemProperties.getProperties();

        Map<String, String> map = fromArgs.toMap();

        // 返回未访问过的key的name set
        fromArgs.getUnrequestedParameters();

        env.getConfig().setGlobalJobParameters(fromArgs);

        env.fromElements("a", "b").map(w -> {
            String prefix = fromArgs.get("prefix", "=>");

            return prefix + w;
        }).map(new RichMapFunction<String, String>() {
            private ParameterTool globalJobParameters;
            @Override
            public void open(Configuration parameters) throws Exception {
                globalJobParameters= (ParameterTool)getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            }

            @Override
            public String map(String value) throws Exception {
                String suffix = globalJobParameters.get("suffix", "<=");

                return value + suffix;
            }
        }).print();

    }


}
