package com.song.spark.project.dao;

import com.song.spark.project.domain.CourseClickCount;
import com.song.spark.project.utils.HBaseUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * @author songshiyu
 * @date 2020/2/15 13:54
 **/
public class CourseClickCountDAO implements Serializable{

    private static String clickcoutTableName = "course_course_clickcout_song";
    private static String cf = "info";
    private static String qualifer = "click_count";

    private static String searchTableName = "course_course_search_clickcout_song";
    /**
     * 保存数据到hbase
     * */
    public static void saveDataToHabse(List<CourseClickCount> clickCountList){
        HTable table = HBaseUtils.getInstance().getTable(clickcoutTableName);
        for (CourseClickCount courseClickCount:clickCountList){
            try {
                table.incrementColumnValue(Bytes.toBytes(courseClickCount.getDay_course()),
                        Bytes.toBytes(cf),Bytes.toBytes(qualifer),courseClickCount.getClick_count());
            } catch (IOException e) {
                e.printStackTrace();
            }finally {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    /**
     * 从hbase进行查询
     * */
    public static Long queryFromHBase(String dayCourse){
        HTable table = HBaseUtils.getInstance().getTable(clickcoutTableName);

        Get get = new Get(Bytes.toBytes(dayCourse));
        try {
            byte[] value = table.get(get).getValue(cf.getBytes(), qualifer.getBytes());
            if (value == null){
                return 0L;
            }else {
                return Bytes.toLong(value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0L;
    }

    public static void main(String[] args) {
        List<CourseClickCount> clickCountList = Arrays.asList(new CourseClickCount("20200216_11", 20L),
                new CourseClickCount("20200216_12", 30L),
                new CourseClickCount("20200216_13", 40L));

        saveDataToHabse(clickCountList);


        Long value1 = queryFromHBase("20200216_11");
        Long value2 = queryFromHBase("20200216_12");
        Long value3 = queryFromHBase("20200216_13");

        System.out.println("20200216_11:" + value1);
        System.out.println("20200216_12:" + value2);
        System.out.println("20200216_13:" + value3);

    }

}
