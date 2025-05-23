package com.lyx.stream.realtime.v2.app.dm;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @Package com.rb.test_dm.true_a.KeyWordGradeMap
 * @Author runbo.zhang
 * @Date 2025/5/14 19:03
 * @description:
 */
public class KeyWordGradeMap  extends RichMapFunction<JSONObject,JSONObject> {


    private static final String keyWordPath= "D:\\07Aflink\\stream_realtime_dev\\stream_realtime\\src\\main\\java\\com\\lyx\\stream\\realtime\\v2\\app\\dm/doc/keyWordWeight.txt";
    private static final String devicePath= "D:\\07Aflink\\stream_realtime_dev\\stream_realtime\\src\\main\\java\\com\\lyx\\stream\\realtime\\v2\\app\\dm/doc/deviceWeight.txt";

    private static HashMap<String,JSONObject> keyWordMap ;
    private static  HashMap<String,JSONObject> deviceMap ;

    static {
        keyWordMap = ReadToJson.readFileToJsonMap(keyWordPath);
        deviceMap = ReadToJson.readFileToJsonMap(devicePath);

    }

    @Override
    public JSONObject map(JSONObject value) throws Exception  {

        String keyword = value.getString("keyword");
        if (keyword.contains("匡威")){
            value.put("search", "时尚与潮流");
        }else if (keyword.contains("衬衫") || keyword.contains("心相印纸抽")){
            value.put("search", "性价比");
        } else if (keyword.contains("扫地机器人")) {
            value.put("search", "家庭与育儿");
        }else if (keyword.contains("拯救者")|| keyword.contains("小米")){
            value.put("search", "数码与科技");
        }else if (keyword.contains("联想")){
            value.put("search", "学习与发展");
        }else {
            value.put("search", "健康与养生");
        }
        //搜索关键词
        String search = value.getString("search");
        JSONObject keywordObj = keyWordMap.get(search);
        value.put("keyword_weight", keywordObj);

        String device_os = value.getString("os").split(",")[0];
        value.put("device_os",device_os);
        JSONObject deviceObj = deviceMap.get(device_os);
        value.put("device_weight", deviceObj);
        return value;




    }
}
