package com.lyx.stream.realitime2.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lyx.stream.realtime.v1.bean.TableProcessDim;
import com.lyx.stream.realtime.v1.constant.Constant;
import com.lyx.stream.realtime.v1.function.HBaseSinkFunction;
import com.lyx.stream.realtime.v1.function.TableProcessFunction;
import com.lyx.stream.realtime.v1.utils.FlinkSourceUtil;
import com.lyx.stream.realtime.v1.utils.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Description: 对kafka数据进行处理和过滤之后存入Hbase
 * @Author: lyx
 * @Date: 2025/5/12 09:58
 */
public class FlinkKafkatoHbase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        env.enableCheckpointing(5000L , CheckpointingMode.EXACTLY_ONCE);

        //从 Kafka 读取数据
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_DB, "dim_app");

        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");

//        kafkaStrDS.print();
        // 处理 Kafka 原始数据流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                //使用 ProcessFunction 对 Kafka 读取的 JSON 字符串进行处理
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {

                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String db = jsonObj.getJSONObject("source").getString("db");
                        String type = jsonObj.getString("op");
                        String data = jsonObj.getString("after");

                        if ("realtime_v1".equals(db)
                                && ("c".equals(type)
                                || "u".equals(type)
                                || "d".equals(type)
                                || "r".equals(type))
                                && data != null
                                && data.length() > 2
                        ) {
                            out.collect(jsonObj);
                        }
                    }
                }
        );

//        jsonObjDS.print();


        //获取 MySQL 数据源
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("realtime_v1_config", "table_process_dim");

        DataStreamSource<String> mysqlStrDS = env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source")
                .setParallelism(1);

//        mysqlStrDS.print();


        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) {
                        // 解析 JSON
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        // 提取操作类型 (op)
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim = null;
                        // 根据类型选择数据字段
                        if("d".equals(op)){
                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                        }else{
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);

//        tpDS.print();


        tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {

                    private Connection hbaseConn;
                    //在 open 方法中获取 HBase 连接
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    //在 close 方法中关闭 HBase 连接
                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tp) {
                        String op = tp.getOp();
                        String sinkTable = tp.getSinkTable();
                        String[] sinkFamilies = tp.getSinkFamily().split(",");
                        if("d".equals(op)){
                            HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE,sinkTable);
                        }else if("r".equals(op)||"c".equals(op)){
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }else{
                            HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }
                        return tp;
                    }
                }
        ).setParallelism(1);

//         tpDS.print();

        //广播 MySQL 数据 用于后续关联主数据流
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
                new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor",String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);


        //连接 Kafka 数据和广播数据
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);

        // 处理连接后的数据
        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcessDim>> dimDS = connectDS
                .process(new TableProcessFunction(mapStateDescriptor));
//        dimDS.print();
        //写入 HBase
        dimDS.addSink(new HBaseSinkFunction());

        env.execute("FlinkKafkatoHbase");

//向表空间 ns_yuxin_li 下的表dim_base_province中put数据33成功
    }
}
