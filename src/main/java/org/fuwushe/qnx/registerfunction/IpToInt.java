package org.fuwushe.qnx.registerfunction;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;

public class IpToInt extends ScalarFunction {


    public IpToInt() {
    }

    public Integer eval(String sip,String sipv6) {
      return ipToInt(sip,sipv6);
    }

    public int ipToInt(String sip, String sipv6) {
        int result = 0;
        if (StringUtils.isNotBlank(sip) || StringUtils.isNotBlank(sipv6)){
            String ipAddress = StringUtils.isBlank(sip)? sipv6:sip;


            String[] ipAddressInArray = ipAddress.split("\\.");

            for (int i = 3; i >= 0; i--) {
                int ip = Integer.parseInt(ipAddressInArray[3 - i]);
                result |= ip << (i * 8);
            }

        }

        return result;
    }
}