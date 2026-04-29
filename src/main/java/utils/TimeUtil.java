package utils;

import java.text.SimpleDateFormat;

public class TimeUtil {
    public static String tsToDate(long ts) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(ts);
    }
}
