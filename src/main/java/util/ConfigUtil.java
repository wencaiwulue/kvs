package util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;

public class ConfigUtil {

    private static Config config;

    public static Config getConfig() {
        if (config == null) {
            synchronized (ConfigUtil.class) {
                if (config == null) {
                    init();
                }
            }
        }
        return config;
    }

    public static class Config {
        private String dir;
    }


    public static void init() {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            config = mapper.readValue(new File("src/main/resources/config.yaml"), Config.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
