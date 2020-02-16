package com.song.spark.project.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author songshiyu
 * @date 2020/2/15 13:58
 * <p>
 * HBase操作工具类  建议使用单例模式进行封装
 **/
@Slf4j
public class HBaseUtils {

    HBaseAdmin admin = null;

    Configuration configuration = null;

    /**
     * 私有构造方法
     */
    private HBaseUtils() {
        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "192.168.137.129:2181");
        configuration.set("hbase.rootdir", "hdfs://192.168.137.129:8020/hbase");

        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HBaseUtils instance = null;

    public static synchronized HBaseUtils getInstance() {
        if (instance == null) {
            instance = new HBaseUtils();
        }
        return instance;
    }

    /**
     * 根据表名获取到HTable实例
     */
    public HTable getTable(String tableName) {
        HTable table = null;
        try {
            table = new HTable(configuration, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    /**
     * 向hbase插入值
     * */
    public void putToHbase(String tableName,String rowkey,String cf,String column,Integer value){
        HTable table = getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value));
        try {
            table.put(put);
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

    public void getRecordsFromHbase(String tableName,String rowKey){
        HTable table = getTable(tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        try {
            Result result = table.get(get);
            String resultValue = Bytes.toString(result.getRow());
            System.out.println(resultValue);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        //HBaseUtils.getInstance().putToHbase("course_course_clickcout_song","20200215_77","info","click_count",2);
        HBaseUtils.getInstance().getRecordsFromHbase("course_course_clickcout_song","20200216_112");
    }
}
