package com.lyx.stream.realtime.v2.app.dm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @Package com.rb.test_dm.Test1
 * @Author runbo.zhang
 * @Date 2025/5/10 10:24
 * @description:
 */
public class OdJoinUser {
    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取dwd层订单明细宽表
        DataStreamSource<String> dwdOrderDetail = SourceSinkUtils.kafkaRead(env, "yuxin_dws_sku_order_detail_v1");
        SingleOutputStreamOperator<JSONObject> jsonDs = dwdOrderDetail.map(o -> JSON.parseObject(o));

        //读取ods数据 过滤用户表 异步io
        SingleOutputStreamOperator<JSONObject> joinUserDs = AsyncDataStream.unorderedWait(jsonDs, new DimAsync<JSONObject>() {
            @Override
            public void addDims(JSONObject obj, JSONObject dimJsonObj) {

                //id,login_name,name,user_level,birthday,gender,create_time,operate_time
                String login_name = dimJsonObj.getString("login_name");
                String name = dimJsonObj.getString("name");
                String birthday = dimJsonObj.getString("birthday");
                String gender = dimJsonObj.getString("gender");
                obj.put("user_login_name", login_name);
                obj.put("user_name", name);
                obj.put("user_birthday", birthday);
                obj.put("user_gender", gender);
            }

            @Override
            public String getTableName() {
                return "dim_user_info";
            }

            @Override
            public String getRowKey(JSONObject obj) {
                return obj.getString("user_id");
            }
        }, 300, TimeUnit.SECONDS);
        joinUserDs.print();
        joinUserDs.map(o->o.toJSONString()).sinkTo(SourceSinkUtils.sinkToKafka("od_join_user"));




        env.disableOperatorChaining();
        env.execute();


    }
}
