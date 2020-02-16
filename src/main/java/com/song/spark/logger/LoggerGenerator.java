package com.song.spark.logger;

import org.apache.log4j.Logger;

/**
 * @author songshiyu
 * @date 2020/2/14 9:36
 *
 *  日志文件对接到flume  需要在log4j.properties中配置
 *  然后flume对接到kafka，看服务器flume 配置文件中flume_kafka.con
 **/
public class LoggerGenerator {
    private static final Logger logger = Logger.getLogger(LoggerGenerator.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        int index = 0;
        while (true){
            Thread.sleep(1000L);
            logger.info("value :" + index++);
        }
    }
}
