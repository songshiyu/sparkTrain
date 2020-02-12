package com.song.spark.util;

import org.apache.commons.dbcp.BasicDataSource;
import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author songshiyu
 * @date 2020/2/12 10:00
 **/
public class MysqlDBPoolUtil implements Serializable{

    /**
     * 建立连接的驱动驱动名称
     */
    public static final String DRIVER_CLASS_NAME = "com.mysql.jdbc.Driver";

    /**
     * 数据库链接数据库的url
     */
    public static final String URL = "jdbc:mysql://localhost:3306/spark?characterEncoding=utf-8&useSSL=false";

    /**
     * 链接的数据库账号
     * */
    public static final String USERNAME = "root";

    /**
     * 链接的数据库密码
     * */
    public static final String PASSWORD = "root";

    /**
     * 最大空闲链接
     * */
    private static final int MAX_IDLE = 50;

    /**
     * 最大的等待时间
     * */
    private static final long MAX_WAIT = 50000;

    /**
     * 最大活动链接
     * */
    private static final int MAX_ACTIVE = 50;

    /**
     * 初始化时链接池的数量
     * */
    private static final int INITIAL_SIZE = 50;

    /**
     * 得到的a到链接实例
     */
    private static BasicDataSource dataSource = new BasicDataSource();

    /**
     * 初始化链接参数
     */
    static{
        dataSource.setDriverClassName(DRIVER_CLASS_NAME);
        dataSource.setUrl(URL);
        dataSource.setUsername(USERNAME);
        dataSource.setPassword(PASSWORD);
        dataSource.setMaxActive(MAX_IDLE);
        dataSource.setMaxWait(MAX_WAIT);
        dataSource.setMaxActive(MAX_ACTIVE);
        dataSource.setInitialSize(INITIAL_SIZE);
    }

    /**
     * 提供获得数据源
     */
    public static DataSource getDateSource(){
        return dataSource;
    }

    /**
     * 提供获得链接
     */
    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
}
