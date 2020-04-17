package org.fuwushe.order.rich;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.fuwushe.item.UserBehaviorVO;

public class MapWithParameters extends RichMapFunction<String,UserBehaviorVO> {

    String behavior;

    @Override
    public UserBehaviorVO map(String value)  {
        UserBehaviorVO vo = JSONObject.parseObject(value,UserBehaviorVO.class);
        ParameterTool parameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        behavior = parameters.getRequired("behavior");
        if (behavior.equals("searchRecord")){
            vo.setItemId(vo.getContent());
        }
        return vo;
    }
}
