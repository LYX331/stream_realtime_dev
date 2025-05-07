package func;
import com.lyx.stream.realtime.v1.constant.Constant;
import com.lyx.stream.realtime.v1.utils.EnvironmentSettingUtils;
import com.lyx.stream.realtime.v1.utils.KafkaUtils;
import com.lyx.stream.realtime.v2.app.utils.FilterBloomDeduplicatorFunc;
import com.lyx.stream.realtime.v2.app.utils.MapCheckRedisSensitiveWordsFunc;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
/**
 * @Description:
 * @Author: lyx
 * @Date: 2025/5/7 16:46
 */
public class DbusBanBlackListUserInfo2Kafka {
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettingUtils.defaultParameter(env);

        // 评论表 取数
        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(
                        // Kafka 集群的地址，用于连接到 Kafka 服务器
                        Constant.KAFKA_BROKERS,
                        //Kafka  主题
                        Constant.TOPIC_GL,
                        // 消费者组的 ID，这里使用当前日期的字符串表示
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                // 这里使用 forBoundedOutOfOrderness 方法创建一个有界乱序水印策略，允许数据乱序的最大时间为 3 秒
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        // 为每个事件分配时间戳
                        .withTimestampAssigner((event, timestamp) -> {
                            // 检查事件是否为空
                            if (event != null) {
                                try {
                                    // 将事件字符串解析为 JSON 对象
                                    // 并从 JSON 对象中获取 "ts_ms" 字段的值作为事件的时间戳
                                    return JSONObject.parseObject(event).getLong("ts_ms");
                                } catch (Exception e) {
                                    // 如果解析 JSON 或获取 "ts_ms" 字段值时发生异常
                                    // 打印异常堆栈信息
                                    e.printStackTrace();
                                    // 输出错误信息，提示解析失败
                                    System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                    // 若解析失败，返回时间戳为 0L
                                    return 0L;
                                }
                            }
                            // 如果事件为空，返回时间戳为 0L
                            return 0L;
                        }),
                "kafka_cdc_xy_source"
        ).uid("kafka_cdc_xy_source").name("kafka_cdc_xy_source");
//        kafkaCdcDbSource.print();

        //      {"op":"c","after":{"payment_way":"3501","refundable_time":1747264827000,"original_total_amount":"24522.00","order_status":"1001","consignee_tel":"13888155719","trade_body":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 4GB+128GB 明月灰 游戏智能手机 小米 红米等7件商品","id":1133,"consignee":"吴琛钧","create_time":1746660027000,"coupon_reduce_amount":"0.00","out_trade_no":"858182663635648","total_amount":"24272.00","user_id":78,"province_id":27,"activity_reduce_amount":"250.00"},"source":{"thread":20259,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000004","connector":"mysql","pos":31459615,"name":"mysql_binlog_source","row":0,"ts_ms":1746596801000,"snapshot":"false","db":"realtime_v1","table":"order_info"},"ts_ms":1746596800964}
        DataStream<JSONObject> filteredOrderInfoStream = kafkaCdcDbSource
                //转成json
                .map(JSON::parseObject)
                //过滤 表中是否是 order_info
                .filter(json -> json.getJSONObject("source").getString("table").equals("order_info"))
                .uid("kafka_cdc_xy_order_source").name("kafka_cdc_xy_order_source");
//        5> {"op":"c","after":{"payment_way":"3501","consignee":"韦明永","create_time":1746654652000,"refundable_time":1747259452000,"original_total_amount":"69.00","coupon_reduce_amount":"0.00","order_status":"1001","out_trade_no":"432269165763778","total_amount":"69.00","user_id":353,"province_id":28,"consignee_tel":"13257824651","trade_body":"CAREMiLLE珂曼奶油小方口红 雾面滋润保湿持久丝缎唇膏 M02干玫瑰等1件商品","id":1103,"activity_reduce_amount":"0.00"},"source":{"file":"mysql-bin.000004","connector":"mysql","pos":31242377,"name":"mysql_binlog_source","thread":20261,"row":0,"server_id":1,"version":"1.9.7.Final","ts_ms":1746596799000,"snapshot":"false","db":"realtime_v1","table":"order_info"},"ts_ms":1746596799639}
//        filteredOrderInfoStream.print();
        // 评论表进行进行升维处理 和hbase的维度进行关联补充维度数据
        DataStream<JSONObject> filteredStream = kafkaCdcDbSource
                .map(JSON::parseObject)
                .filter(json -> json.getJSONObject("source").getString("table").equals("comment_info"))
                .keyBy(json -> json.getJSONObject("after").getString("appraise"));
        // {"op":"c","after":{"create_time":1746624077000,"user_id":178,"appraise":"1201","comment_txt":"评论内容：44237268662145286925725839461514467765118653811952","nick_name":"珠珠","sku_id":14,"id":85,"spu_id":4,"order_id":1010},"source":{"file":"mysql-bin.000004","connector":"mysql","pos":30637591,"name":"mysql_binlog_source","thread":20256,"row":0,"server_id":1,"version":"1.9.7.Final","ts_ms":1746596796000,"snapshot":"false","db":"realtime_v1","table":"comment_info"},"ts_ms":1746596796319}
        filteredStream.print();

        env.execute();
    }
}
