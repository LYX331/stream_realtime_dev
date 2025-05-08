package com.lyx.stream.realtime.v2.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lyx.stream.realtime.v1.bean.UserLoginBean;
import com.lyx.stream.realtime.v1.function.BeanToJsonStrMapFunction;
import com.lyx.stream.realtime.v1.utils.DateFormatUtil;
import com.lyx.stream.realtime.v1.utils.FlinkSinkUtil;
import com.lyx.stream.realtime.v1.utils.FlinkSourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Package com.lyx.stream.realtime.v2.app.dws.DwsUserUserLoginWindow
 * @Author yuxin_li
 * @Date 2025/4/21 9:56
 * @description: DwsUserUserLoginWindow
 * 从 Kafka 主题读取用户登录相关数据
 * 统计每 10 秒内的新增用户数和回流用户数
 * 对数据进行过滤、状态管理、窗口聚合等处理，最后将处理结果写入到 Doris 数据库
 */

public class DwsUserUserLoginWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        // 从名为 "dwd_traffic_page_yuxin_li" 的 Kafka 主题消费数据，使用 "dws_user_user_login_window" 作为消费者组
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_traffic_page_yuxin_li", "dws_user_user_login_window");
        // 从 Kafka 数据源读取数据，创建一个 DataStreamSource 对象，使用无水印策略，
        // 并将该数据源命名为 "Kafka_Source"
        DataStreamSource<String> kafkaStrDS
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");
        // 将从 Kafka 读取的字符串数据转换为 JSONObject 对象，使用 JSON.parseObject 方法进行解析
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

//        jsonObjDS.print();
        // 对 jsonObjDS 进行过滤操作，筛选出满足特定条件的数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        String uid = jsonObj.getJSONObject("common").getString("uid");
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        return StringUtils.isNotEmpty(uid)
                                && ("login".equals(lastPageId) || StringUtils.isEmpty(lastPageId));
                    }
                }
        );

//        filterDS.print();

        // 为 filterDS 分配时间戳和水印，使用单调递增时间戳策略，
        // 从 JSON 对象中提取时间戳字段 "ts" 作为事件时间
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = filterDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts");
                                    }
                                }
                        )
        );
        // 对 withWatermarkDS 按照用户 ID 进行分组，创建一个 KeyedStream 对象
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("uid"));

        // 对 keyedDS 应用 KeyedProcessFunction 进行处理，将 JSON 对象转换为 UserLoginBean 对象
        SingleOutputStreamOperator<UserLoginBean> beanDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
                    // 定义一个状态变量，用于存储用户的最后登录日期
                    private ValueState<String> lastLoginDateState;

                    @Override
                    public void open(Configuration parameters) {
                        // 创建一个状态描述符，指定状态名称为 "lastLoginDateState"，状态类型为 String
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<>("lastLoginDateState", String.class);
                        // 从运行时上下文获取状态对象
                        lastLoginDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context ctx, Collector<UserLoginBean> out) throws Exception {
                        // 获取用户的最后登录日期
                        String lastLoginDate = lastLoginDateState.value();
                        // 从 JSON 对象中提取时间戳
                        Long ts = jsonObj.getLong("ts");
                        // 将时间戳转换为日期字符串
                        String curLoginDate = DateFormatUtil.tsToDate(ts);

                        // 定义两个计数器，分别用于记录新增用户数和回流用户数
                        long uuCt = 0L;
                        long backCt = 0L;
                        if (StringUtils.isNotEmpty(lastLoginDate)) {
                            // 如果最后登录日期不为空，且与当前登录日期不同
                            if (!lastLoginDate.equals(curLoginDate)) {
                                //// 新增用户数加 1
                                uuCt = 1L;
                                // 更新最后登录日期状态
                                lastLoginDateState.update(curLoginDate);
                                // 计算距离上次登录的天数
                                long day = (ts - DateFormatUtil.dateToTs(lastLoginDate)) / 1000 / 60 / 60 / 24;
                                // 如果距离上次登录超过 8 天，则回流用户数加 1
                                if (day >= 8) {
                                    backCt = 1L;
                                }
                            }
                        } else {
                            // 如果最后登录日期为空，说明是新用户，新增用户数加 1
                            uuCt = 1L;
                            // 更新最后登录日期状态
                            lastLoginDateState.update(curLoginDate);
                        }
                        // 如果新增用户数不为 0，则将 UserLoginBean 对象收集输出
                        if (uuCt != 0L) {
                            out.collect(new UserLoginBean("", "", "", backCt, uuCt, ts));
                        }
                    }
                }
        );

//        beanDS.print();
        // 对 beanDS 应用全窗口操作，使用滚动事件时间窗口，窗口大小为 10 秒
        AllWindowedStream<UserLoginBean, TimeWindow> windowDS = beanDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        // 对 windowDS 进行聚合操作，使用 ReduceFunction 对窗口内的数据进行聚合，
        // 并使用 AllWindowFunction 对聚合结果进行进一步处理
        SingleOutputStreamOperator<UserLoginBean> reduceDS = windowDS.reduce(
                new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) {
                        // 将两个 UserLoginBean 对象的新增用户数和回流用户数相加
                        value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        return value1;
                    }
                },
                new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) {
                        // 获取窗口内的第一个 UserLoginBean 对象
                        UserLoginBean bean = values.iterator().next();
                        // 将窗口的开始时间和结束时间转换为日期时间字符串
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        // 将窗口的开始时间转换为日期字符串
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        // 设置 UserLoginBean 对象的开始时间、结束时间和当前日期
                        bean.setStt(stt);
                        bean.setEdt(edt);
                        bean.setCurDate(curDate);
                        // 收集处理后的 UserLoginBean 对象
                        out.collect(bean);
                    }
                }
        );
        // 将 reduceDS 中的 UserLoginBean 对象转换为 JSON 字符串
        SingleOutputStreamOperator<String> jsonMap = reduceDS
                .map(new BeanToJsonStrMapFunction<>());

//        jsonMap.print();

        // 将 jsonMap 中的数据写入 Doris 数据库，使用自定义工具类 FlinkSinkUtil 的 getDorisSink 方法创建 Doris 数据源
        jsonMap.sinkTo(FlinkSinkUtil.getDorisSink("dws_user_user_login_window"));

        env.execute("DwsUserUserLoginWindow");
    }
}
