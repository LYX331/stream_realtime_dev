package com.lyx.stream.realtime.v2.app.dm;

import com.alibaba.fastjson.JSONObject;

/**
 * @Package
 * @Author
 * @Date 2025/4/22 22:12
 * @description:
 */
public interface DimJoinFunction<T> {
    /***
     * @author:
     * @description: 具体添加字段
     * @params: [T, com.alibaba.fastjson.JSONObject]
     * @return: void
     * @date: 2025/4/22 22:52
     */
    void addDims(T obj, JSONObject dimJsonObj) ;

    /**
     * @author:
     * @description: 给出表名
     * @params: []
     * @return: java.lang.String
     * @date: 2025/4/22 22:52
     */
    String getTableName() ;
    /**
     * @author:
     * @description: 给出流中连接字段
     * @params: [T]
     * @return: java.lang.String
     * @date: 2025/4/22 22:52
     */
    String getRowKey(T obj) ;
}