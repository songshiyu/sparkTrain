package com.song.spark.project.domain;

import lombok.Data;

/**
 * @author songshiyu
 * @date 2020/2/16 9:27
 * 从搜索引擎过来的
 **/
@Data
public class CourseSearchClickCount {

    private String day_search;

    private Long click_count;

}


