package util;

import java.io.File;

/**
 * @author naison
 * @since 4/28/2020 20:47
 */
public class FileUtil {

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void rm(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File temp : files) {
                    rm(temp);
                }
            }
        }
        file.delete();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void emptyFolder(File folder) {
        if (!folder.exists()) {
            folder.mkdirs();
        } else {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File temp : files) {
                    rm(temp);
                }
            }
        }
    }
}
