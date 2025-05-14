package com.lyx.stream.realitime2.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lyx.stream.realtime.v2.app.utils.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @Description:
 * @Author: lyx
 * @Date: 2025/5/12 21:51
 */


// 假设这是一个处理Kafka数据并进行数据转换的类
public class BasicProfileable {
            // 移除源字段并处理数据，返回处理后的单输出流
            public static SingleOutputStreamOperator<JSONObject> removeSourceFields(SingleOutputStreamOperator<JSONObject> userInfoOutputDS1) {
                // 使用map函数对输入流中的每个JSONObject进行处理
                return userInfoOutputDS1.map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        // 获取jsonObject中的"after"字段对应的JSONObject
                        JSONObject after = jsonObject.getJSONObject("after");
                        // 向after对象中添加"op"字段，值为原jsonObject中的"op"字段
                        after.put("op", jsonObject.getString("op"));
                        // 向after对象中添加"ts"字段，值为原jsonObject中的"ts_ms"字段
                        after.put("ts", jsonObject.getString("ts_ms"));
                        // 返回处理后的after对象
                        return after;
                    }
                });
            }

            // 为侧边输出流分配时间戳和水位线
            public static SingleOutputStreamOperator<JSONObject> assignTimestampsAndWatermarks(SideOutputDataStream<JSONObject> userInfoOutputDS) {
                // 使用WatermarkStrategy为流分配单调递增的时间戳
                return userInfoOutputDS.assignTimestampsAndWatermarks(
                        WatermarkStrategy
                            .<JSONObject>forMonotonousTimestamps()
                            .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                                @Override
                                public long extractTimestamp(JSONObject jsonObject, long l) {
                                    // 从jsonObject中提取"ts_ms"字段作为时间戳
                                    return jsonObject.getLong("ts_ms");
                                }
                            })
                );
            }

            @SneakyThrows
            public static void main(String[] args) {
                // 获取流执行环境
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(4);

                // 从Kafka获取数据源，这里假设KafkaUtil.getKafkaSource方法能正确获取数据
                DataStreamSource<String> kafkaSource = KafkaUtil.getKafkaSource(env, "yuxin_li_db", "BasicProfileable");

                /// 定义输出标签，用于提取订单信息
                // 过滤出订单信息
                final OutputTag<JSONObject> orderInfoJsonDS = new OutputTag<JSONObject>("order_info") {
                };
                // 定义输出标签，用于提取购物车信息
                // 过滤出购物车信息
                final OutputTag<JSONObject> cartInfoJsonDS = new OutputTag<JSONObject>("cart_info") {
                };
                // 定义输出标签，用于提取订单详情信息
                // 过滤出订单详情信息
                final OutputTag<JSONObject> orderDetailJsonDS = new OutputTag<JSONObject>("order_detail") {
                };
                // 定义输出标签，用于提取评论信息
                // 过滤出评论信息
                final OutputTag<JSONObject> commentInfoJsonDS = new OutputTag<JSONObject>("comment_info") {
                };
                // 定义输出标签，用于提取收藏信息
                // 过滤出收藏信息
                final OutputTag<JSONObject> favorInfoJsonDS = new OutputTag<JSONObject>("favor_info") {
                };
                // 定义输出标签，用于提取用户信息补充消息
                final OutputTag<JSONObject> userInfoSupMsgJsonDS = new OutputTag<JSONObject>("user_info_sup_msg") {
                };
                // 定义输出标签，用于提取用户信息
                final OutputTag<JSONObject> userInfoJsonDS = new OutputTag<JSONObject>("user_info") {
                };

                // 将Kafka中的数据解析为JSONObject，并根据表名进行分流处理
                SingleOutputStreamOperator<Object> dbJsonDS = kafkaSource.map(JSON::parseObject).process(new ProcessFunction<JSONObject, Object>() {
                    @Override
                    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, Object>.Context context, Collector<Object> collector) throws Exception {
                        // 获取jsonObject中"source"字段对应的JSONObject中的"table"字段
                        String table = jsonObject.getJSONObject("source").getString("table");
                        // 根据表名进行条件判断，将数据输出到对应的侧输出流
                        if (table.equals("order_info")) {
                            context.output(orderInfoJsonDS, jsonObject);
                        } else if (table.equals("order_detail")) {
                            context.output(orderDetailJsonDS, jsonObject);
                        } else if (table.equals("cart_info")) {
                            context.output(cartInfoJsonDS, jsonObject);
                        } else if (table.equals("comment_info")) {
                            context.output(commentInfoJsonDS, jsonObject);
                        } else if (table.equals("favor_info")) {
                            context.output(favorInfoJsonDS, jsonObject);
                        } else if (table.equals("user_info_sup_msg")) {
                            context.output(userInfoSupMsgJsonDS, jsonObject);
                        } else if (table.equals("user_info")) {
                            context.output(userInfoJsonDS, jsonObject);
                        }
                        // 将数据收集到主输出流中
                        collector.collect(jsonObject);
                    }
                });
                // 获取各个侧输出流的数据
                // 获取测流数据
                SideOutputDataStream<JSONObject> orderInfoOutputDS = dbJsonDS.getSideOutput(orderInfoJsonDS);
                SideOutputDataStream<JSONObject> cartInfoOutputDS = dbJsonDS.getSideOutput(cartInfoJsonDS);
                SideOutputDataStream<JSONObject> orderDetailOutputDS = dbJsonDS.getSideOutput(orderDetailJsonDS);
                SideOutputDataStream<JSONObject> commentInfoOutputDS = dbJsonDS.getSideOutput(commentInfoJsonDS);
                SideOutputDataStream<JSONObject> favorInfoOutputDS = dbJsonDS.getSideOutput(favorInfoJsonDS);
                SideOutputDataStream<JSONObject> userInfoSupMsgOutputDS = dbJsonDS.getSideOutput(userInfoSupMsgJsonDS);
                SideOutputDataStream<JSONObject> userInfoOutputDS = dbJsonDS.getSideOutput(userInfoJsonDS);

                // 为各个侧输出流分配时间戳和水位线
                //水位线
                SingleOutputStreamOperator<JSONObject> orderInfoOutputDS1 = assignTimestampsAndWatermarks(orderInfoOutputDS);
                SingleOutputStreamOperator<JSONObject> cartInfoOutputDS1 = assignTimestampsAndWatermarks(cartInfoOutputDS);
                SingleOutputStreamOperator<JSONObject> orderDetailOutputDS1 = assignTimestampsAndWatermarks(orderDetailOutputDS);
                SingleOutputStreamOperator<JSONObject> commentInfoOutputDS1 = assignTimestampsAndWatermarks(commentInfoOutputDS);
                SingleOutputStreamOperator<JSONObject> favorInfoOutputDS1 = assignTimestampsAndWatermarks(favorInfoOutputDS);
                SingleOutputStreamOperator<JSONObject> userInfoSupMsgOutputDS1 = assignTimestampsAndWatermarks(userInfoSupMsgOutputDS);
                SingleOutputStreamOperator<JSONObject> userInfoOutputDS1 = assignTimestampsAndWatermarks(userInfoOutputDS);

                // 对各个流进行字段优化，移除源字段
                // 对字段进行优化（过滤）
                SingleOutputStreamOperator<JSONObject> userInfoOutputDS2 = removeSourceFields(userInfoOutputDS1);
                SingleOutputStreamOperator<JSONObject> userInfoSupMsgOutputDS2 = removeSourceFields(userInfoSupMsgOutputDS1);
                SingleOutputStreamOperator<JSONObject> favorInfoOutputDS2 = removeSourceFields(favorInfoOutputDS1);
                SingleOutputStreamOperator<JSONObject> commentInfoOutputDS2 = removeSourceFields(commentInfoOutputDS1);
                SingleOutputStreamOperator<JSONObject> orderDetailOutputDS12 = removeSourceFields(orderDetailOutputDS1);
                SingleOutputStreamOperator<JSONObject> cartInfoOutputDS2 = removeSourceFields(cartInfoOutputDS1);
                SingleOutputStreamOperator<JSONObject> orderInfoOutputDS2 = removeSourceFields(orderInfoOutputDS1);

                // 对用户信息流和用户信息补充消息流进行关联操作
                // 异步io
                SingleOutputStreamOperator<JSONObject> userInfo = userInfoOutputDS2
                    .keyBy(o -> o.getString("id"))
                    .intervalJoin(userInfoSupMsgOutputDS2.keyBy(o -> o.getString("uid")))
                    .between(Time.minutes(-30), Time.minutes(30))
                    .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                        @Override
                        public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                            // 对用户信息中的生日字段进行转换
                            // 对数据进行转换，如用户的生日进行转换
                            if (jsonObject1 != null && jsonObject1.containsKey("birthday")) {
                                Integer epochDay = jsonObject1.getInteger("birthday");
                                if (epochDay != null) {
                                    LocalDate date = LocalDate.ofEpochDay(epochDay);
                                    jsonObject1.put("birthday", date.format(DateTimeFormatter.ISO_DATE));

                                }
                            }
                            if (jsonObject1 != null) {
                                jsonObject1.put("user_info_sup_msg", jsonObject2);
                                String string = jsonObject1.getString("birthday");
                                String substring = string.substring(0, 3);
                                String substring1 = string.substring(0, 4);

                                System.out.println(substring1);
                                // 添加年代信息
                                // 获取年份
                                jsonObject1.put("nianDai", substring + "0");

                                // 添加年龄信息
                                // 获取年龄
                                jsonObject1.put("age", substring1);
                                jsonObject1.put("age", LocalDate.now().getYear() - jsonObject1.getInteger("age"));

                                // 处理性别信息，若为空则设置为"H"
                                // 获取性别
                                if (jsonObject1.getString("gender") == null) {
                                    jsonObject1.put("gender", "H");
                                }
                            }
                            // 收集处理后的用户信息
                            collector.collect(jsonObject1);

                        }
                    });
                    // 打印处理后的用户信息流
                    userInfo.print();

                    env.execute("BasicProfileable");
    }
}
