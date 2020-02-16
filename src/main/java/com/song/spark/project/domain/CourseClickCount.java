package com.song.spark.project.domain;

import lombok.Data;

/**
 * @author songshiyu
 * @date 2020/2/15 13:52
 *
 * 课程点击数
 **/
@Data
public class CourseClickCount {

    private String day_course;

    private Long click_count;

    public CourseClickCount(String day_course, Long click_count) {
        this.day_course = day_course;
        this.click_count = click_count;
    }
}
