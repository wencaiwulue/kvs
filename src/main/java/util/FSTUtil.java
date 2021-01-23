package util;

import org.nustaq.serialization.FSTConfiguration;

/**
 * @author naison
 * @since 3/25/2020 17:25
 */
public class FSTUtil { // use for communication serialization

    private static final FSTConfiguration BINARY_CONF = FSTConfiguration.createUnsafeBinaryConfiguration();
    private static final FSTConfiguration JSON_CONF = FSTConfiguration.createJsonNoRefConfiguration();

    public static FSTConfiguration getBinaryConf() {
        return BINARY_CONF;
    }

    public static FSTConfiguration getJsonConf() {
        return JSON_CONF;
    }


}
