package org.fuwushe.qnx.registerfunction;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;

public class IfNull extends ScalarFunction {

    public static final int IPV_6 = 6;
    public static final int IPV_4 = 4;

    public IfNull() {
    }

    /**
     * 返回非空值
     * @param sip
     * @param sipv6
     * @return
     */
    public String eval(String sip,String sipv6){
        return StringUtils.isBlank(sip)?sipv6:sip;
    }

    /**
     * 返回指定值 ip类型
     * @param sip
     * @return
     */
    public int eval(String sip){
        return StringUtils.isBlank(sip)? IPV_6 : IPV_4;
    }
}

