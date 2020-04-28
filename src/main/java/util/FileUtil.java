package util;

import java.io.File;

/**
 * @author naison
 * @since 4/28/2020 20:47
 */
public class FileUtil {

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void rm(File file) {
        if (file.isFile()) {
            file.delete();
            return;
        }
        File[] files = file.listFiles();
        if (files != null) {
            for (File temp : files) {
                rm(temp);
            }
        }
    }
}
