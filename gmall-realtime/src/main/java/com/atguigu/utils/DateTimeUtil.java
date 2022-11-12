package com.atguigu.utils;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateTimeUtil {
    public static final DateTimeFormatter formater = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String toYMDhms(Date date){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return formater.format(localDateTime); // 2022-06-06T17:14:26.392 -> 2022-06-06 17:14:26
    }

    public static Long toTs(String YmDHms){
        LocalDateTime parse = LocalDateTime.parse(YmDHms, formater);
        return parse.toInstant(ZoneOffset.of("+8")).toEpochMilli(); // 2022-06-06 17:14:26 -> 1654506866000
    }

    public static void main(String[] args) {
        LocalDate d = LocalDate.now(); // 当前日期
        LocalTime t = LocalTime.now(); // 当前时间
        LocalDateTime dt = LocalDateTime.now(); // 当前日期和时间
        System.out.println(d); // 严格按照ISO 8601格式打印
        System.out.println(t); // 严格按照ISO 8601格式打印
        System.out.println(dt); // 严格按照ISO 8601格式打印

        System.out.println("=================");

        Date date = new Date();
        System.out.println(date);
        String s = DateTimeUtil.toYMDhms(date);
        System.out.println(s);

        Long aLong = DateTimeUtil.toTs(s);
        System.out.println(aLong);

    }
}
