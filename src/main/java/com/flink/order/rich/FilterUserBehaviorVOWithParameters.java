package com.flink.order.rich;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import com.flink.item.UserBehaviorVO;

public class FilterUserBehaviorVOWithParameters extends RichFilterFunction<UserBehaviorVO> {


    String behavior;

    @Override
    public boolean filter(UserBehaviorVO userBehaviorVO)  {
        if (StringUtils.isBlank(userBehaviorVO.getItemId())){
            return false;
        }
        ParameterTool parameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        behavior = parameters.getRequired("behavior");
        return behavior.equals(userBehaviorVO.getBehavior());
    }
}
