package tool;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class DateTool {


    public final static DateTimeFormatter FORMATTER1 =  DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String fmtMilli1(long timeStamp){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timeStamp), ZoneId.systemDefault());
        return localDateTime.format(FORMATTER1);
    }

    public static String fmtSec1(long timeStamp){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(timeStamp), ZoneId.systemDefault());
        return localDateTime.format(FORMATTER1);
    }

    public static long getTimeMilli(String dateTime){
        LocalDateTime dt = LocalDateTime.parse(dateTime, FORMATTER1);
        return dt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    public static long getTimeSec(String dateTime){
        LocalDateTime dt = LocalDateTime.parse(dateTime, FORMATTER1);
        return dt.toEpochSecond(ZoneOffset.of("+8"));
    }

    public static void main(String[] args) {
        System.out.println(getTimeSec("2022-01-22 13:51:30"));
        System.out.println(getTimeMilli("2022-01-22 13:51:30"));

    }
}
