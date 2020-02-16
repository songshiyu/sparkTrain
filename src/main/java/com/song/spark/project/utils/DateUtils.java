package com.song.spark.project.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @author songshiyu
 * @date 2020/2/15 11:34
 *
 * 日期时间工具类
 **/
public class DateUtils {

    public static final SimpleDateFormat start_sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final SimpleDateFormat end_sdf = new SimpleDateFormat("yyyyMMddHHmmss");

    public static String formatDate(String time) throws ParseException {
        return end_sdf.format(start_sdf.parse(time));
    }

    public static void main(String[] args) throws ParseException {
        String s = formatDate("2020-02-15 11:40:01");
        System.out.println(s);
    }
}
