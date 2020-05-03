package util;

/**
 * @author naison
 * @since 5/3/2020 19:50
 */
public class ParseUtil {
    public static int parseInt(String s) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
