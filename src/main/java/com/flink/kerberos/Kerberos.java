package com.flink.kerberos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

public class Kerberos {
    /**
     * 以jass.conf文件认证kerberos
     */
    public static void getKerberosJaas(){
        System.setProperty("java.security.krb5.conf", "/load/data/krb5.conf");
        //加载本地jass.conf文件
        System.setProperty("java.security.auth.login.config", "/load/data/jaas.conf");

        //System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        //System.setProperty("sun.security.krb5.debug","true");
        //System.setProperty("java.security.auth.login.config", "/load/data/flink.keytab");

    }
    public static void getKerberosHdfs() throws IOException {

        System.setProperty("java.security.krb5.conf", "/load/data/krb5.conf");
        //System.setProperty("sun.security.krb5.debug", "true");

        Configuration conf = new Configuration();
//        conf.addResource("/etc/hadoop/conf/core-site.xml");
//        conf.addResource("/etc/hadoop/conf/hdfs-site.xml");
        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("flink/master@AQ.COM", "/load/data/flink-master.keytab");
    }
}
