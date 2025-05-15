package com.lyx.stream.realtime.v2.app.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Description:
 * @Author: lyx
 * @Date: 2025/5/14 14:30
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimBaseCategory implements Serializable {

    private String id;
    private String b3name;
    private String b2name;
    private String b1name;


}
