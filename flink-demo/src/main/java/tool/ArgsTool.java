package tool;

import lombok.SneakyThrows;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public final class ArgsTool {

    private static final Logger logger = LoggerFactory.getLogger(ArgsTool.class);

    private ArgsTool() {
    }

    public static ParameterTool parameterTool(String path) throws IOException {
        return ParameterTool.fromPropertiesFile(path);

    }

    public static ParameterTool parameterTool(File file) throws IOException {

        return ParameterTool.fromPropertiesFile(file);
    }

    public static ParameterTool parameterTool(FileInputStream fileInputStream) throws IOException {
        return ParameterTool.fromPropertiesFile(fileInputStream);

    }

    @SneakyThrows
    public static void main(String[] args) {
        ParameterTool parameter = null;
        try {
            parameter = parameterTool("E:\\DEV\\learning\\flink-demo\\input\\config\\myjob.properties");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("get parameterTool error");
            throw e;
        }

        System.out.println(parameter.getRequired("input"));
        System.out.println(parameter.getLong("expectedCount", -1L));
        System.out.println(parameter.getLong("expectedCoun", -1L));
        System.out.println(parameter.getInt("mapParallelism", 2));

    }

}
