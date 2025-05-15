package com.lyx.stream.realtime.v2.app.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Description:
 * @Author: lyx
 * @Date: 2025/5/15 09:11
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimBaseCategory2 implements Serializable {

    private String id;
    private String bcname;
    private String btname;
}
