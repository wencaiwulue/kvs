package util;

/**
 * @author naison
 * @since 4/12/2020 11:09
 */
public class Util {
    public static final int CPUS = /*Runtime.getRuntime().availableProcessors()*/ 4;
    public static final int MIN_THREAD_POOL_SIZE = CPUS;
    public static final int MAX_THREAD_POOL_SIZE = MIN_THREAD_POOL_SIZE * 2;
    public static final int MAX_SCHEDULED_THREAD_POOL_SIZE = 4;
}
