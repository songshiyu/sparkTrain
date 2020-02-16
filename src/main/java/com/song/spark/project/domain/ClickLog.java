package com.song.spark.project.domain;

import lombok.Data;

import java.io.Serializable;

/**
 * @author songshiyu
 * @date 2020/2/15 12:26
 *
 * 清洗后的日志信息
 **/
@Data
public class ClickLog implements Serializable{

    private String ip;

    private String time;

    private Integer courseId;

    private Integer statusCode;

    private String referer;

    public ClickLog(String ip, String time, Integer courseId, Integer statusCode, String referer) {
        this.ip = ip;
        this.time = time;
        this.courseId = courseId;
        this.statusCode = statusCode;
        this.referer = referer;
    }
}
