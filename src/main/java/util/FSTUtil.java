package util;

import org.nustaq.serialization.FSTConfiguration;

/**
 * @author naison
 * @since 3/25/2020 17:25
 */
public class FSTUtil {

    private static final FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();

    public static FSTConfiguration getConf() {
        return conf;
    }
}
