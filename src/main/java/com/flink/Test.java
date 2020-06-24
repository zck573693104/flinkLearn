package com.flink;

import com.alibaba.fastjson.JSONObject;
import com.flink.drools.Person;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.drools.core.impl.InternalKnowledgeBase;
import org.drools.core.impl.KnowledgeBaseFactory;
import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.Message;
import org.kie.api.definition.KiePackage;
import org.kie.api.io.Resource;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.internal.builder.KnowledgeBuilder;
import org.kie.internal.builder.KnowledgeBuilderError;
import org.kie.internal.builder.KnowledgeBuilderErrors;
import org.kie.internal.builder.KnowledgeBuilderFactory;
import org.kie.internal.io.ResourceFactory;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

public class Test {
    public static void main(String []args) throws Exception {
        ;
        System.out.println(Hashing.murmur3_128().hashUnencodedChars("abc").padToLong());
        System.out.println(Hashing.murmur3_128().hashString("abc", StandardCharsets.UTF_8).asLong());
        String sourceDDL = "CREATE TABLE sourceTable (userId VARCHAR, eventType VARCHAR) WITH (\n"
                + "\t'connector.type' = 'kafka',\n" + "\t'connector.version' = 'universal',\n"
                + "\t'connector.startup-mode' = 'earliest-offset',\n" + "\t'connector.topic' = 'browTopic',\n"
                + "\t  'connector.properties.group.id' = 'testGroup',\n"
                + "\t'connector.properties.zookeeper.connect' = 'localhost:2181',\n"
                + "\t'connector.properties.bootstrap.servers' = 'localhost:9092',\n" + "\t'update-mode' = 'append',\n"
                + "\t'format.type' = 'json',\n" + "\t'format.derive-schema' = 'true'\n" + ")";
        System.out.println(sourceDDL);
        String sinkDDL =
                " CREATE TABLE sinkTable (\n" + "    userId VARCHAR,\n" + "    eventType VARCHAR\n" + ") WITH (\n"
                        + "    'connector.type' = 'jdbc',\n"
                        + "    'connector.url' = 'jdbc:mysql://localhost:3306/flink_test?autoReconnect=true&failOverReadOnly=false&useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8',\n"
                        + "    'connector.table' = 'sinkTable',\n" + "    'connector.username' = 'root',\n"
                        + "    'connector.password' = '123456',\n" + "    'connector.write.flush.max-rows' = '1'\n"
                        + ") ";

        String sinkSql = "insert into sinkTable select * from sourceTable";
        System.out.println(sinkDDL);
        System.out.println(sinkSql);

        String demo = "{\n" +
                " \n" +
                "          \"vul_name\" :\"Local path disclosure\",\n" +
                "          \"common_title\" :\"页面异常导致本地路径泄漏\",\n" +
                "          \"vul_level\" : 1,\n" +
                "          \"industry_id\" :\"41\",\n" +
                "          \"real_time\" :\"2019-04-15T15:48:00.299+0800\",\n" +
                "          \"description\" :\"<p>由于页面异常<strong>（如404页面）</strong>，在报错信息中包含了本地路径。</p><p>1.本地路径泄漏漏洞允许恶意攻击者获取服务器上的WEB根目录的全路径<span style=\\\"color:#ff0000;\\\">（通常在出错信息中）</span>。</p>\",\n" +
                "          \"event_subtype\" :\"000011\",\n" +
                "          \"dprovince\" :\"120000\",\n" +
                "          \"event_token\" : [\"625bb177-ae8f-465d-8e4a-fccfaae54189\"],\n" +
                "          \"event_type\" :\"000\",\n" +
                "          \"solution\" :\"<p>如果WEB应用程序自带错误处理/管理系统，请确保功能开启；否则按语言、环境，分别进行处理：<br />1、如果是PHP应用程序/Apache服务器，可以通过修改php脚本、配置php.ini以及httpd.conf中的配置项来禁止显示错误信息：<br /> 修改php.ini中的配置行: display_errors = off<br />修改httpd.conf/apache2.conf中的配置行: php_flag display_errors off<br />修改php脚本，增加代码行: ini_set('display_errors', false);</p><p>2、如果是IIS 并且是 支持aspx的环境,可以在网站根目录新建web.config文件(存在该文件则直接修改),或者可以参考这里：<a href=\\\"http://bbs.webscan.360.cn/forum.php?mod=viewthread&amp;tid=4560&amp;extra=page%3D1\\\">http://bbs.webscan.360.cn/forum.php?mod=viewthread&amp;tid=4560&amp;extra=page%3D1</a></p><p><strong><span\n" +
                "style=\\\"font-size:18px;\\\">PS：《360网站安全检测》会去猜测敏感文件，如果您被报此漏洞，但又确实不存在提示的文件或路径的，只要关闭服务器的显示报错即可。</span></strong></p>\",\n" +
                "          \"d_location\" :\"39.018741,117.644061\",\n" +
                "          \"vendor\" : \"XXX\",\n" +
                "          \"possible_attack\" :\"<p>恶意攻击者通过利用本地路径信息，在配合其它漏洞对目标服务器实施进一步的攻击。注意：<span style=\\\"font-size:10px;\\\">《360网站安全检测》会去猜测敏感文件，如果您被报此漏洞，但又确实不存在提示的文件或路径的，只要关闭服务器的显示报错即可。站长们只需关注异常报错显示的文件路径。</span></p>\",\n" +
                "          \"dcounty\" :\"120902\",\n" +
                "          \"dcity\" :\"120100\",\n" +
                "          \"affect_url\" :\"http://tjtgsf.com/plus/view.php\",\n" +
                "          \"verifiability\" :\"精准扫描 - 可利用\",\n" +
                "          \"industry_name\" :\"水利\",\n" +
                "          \"method\" :\"get/post\",\n" +
                "          \"system_id\" :\"4075\",\n" +
                "          \"md5_id\" :\"3d78d47dde871daf637ce6f0190ada6d\",\n" +
                "          \"vul_code\" :\"\",\n" +
                "          \"scan_evidence\" :\"726d0f38-5f52-11e9-81e4-6c92bf8d86a6\",\n" +
                "          \"organization_name\" :\"XXX中法供水有限公司\",\n" +
                "          \"vul_type\" :\"51\",\n" +
                "          \"url\" :\"http://tjtgsf.com/plus/view.php.bak\",\n" +
                "          \"found_time\" :\"2019-04-15T15:32:39.000+0800\",\n" +
                "          \"event_id\" :\"726d0f38-5f52-11e9-81e4-6c92bf8d86a6\",\n" +
                "          \"domain\" :\"tjtgsf.com\",\n" +
                "          \"organization_id\" :\"4517\",\n" +
                "          \"data_type\" :\"vulnerability\",\n" +
                "          \"event_level\" : 0,\n" +
                "          \"@timestamp\" :\"2019-04-15T15:48:15.109+0800\",\n" +
                "          \"@version\" : \"6\"\n" +
                "}";
        System.out.println(JSONObject.parseObject(demo.replaceAll("\r\n","")).toString());;

        StringBuffer stringBuffer = new StringBuffer();
        if (1==1){
             stringBuffer.append("1");
        }
        if (2==2){
             stringBuffer.append("2");
        }
        System.out.println(stringBuffer.toString());
        testKey("1","2");
        String []kes = {"1","3"};
        testKey(kes);
        String a = "[你好24,24, ,25]";
        lineToColumn(a,",",2);
        drools();

    }

    public static void testKey(String ...fields){
        priKey(fields);
    }

    public static void priKey(String [] keys) {
        for (String value:keys){
            System.out.println(value);
        }

    }

    public static void lineToColumn(String line,String split,int size){
        for (String value:line.split(split)){
            StringBuilder sb = new StringBuilder();
        }
    }

    public static void drools() throws Exception {
        KnowledgeBuilder kbuilder = KnowledgeBuilderFactory.newKnowledgeBuilder();
        kbuilder.add(ResourceFactory.newClassPathResource("rules/Test.drl"), ResourceType.DRL);

        KnowledgeBuilderErrors errors = kbuilder.getErrors();
        if (errors.size() > 0) {
            for (KnowledgeBuilderError error: errors) {
                System.err.println(error);
            }
            throw new IllegalArgumentException("Could not parse knowledge.");
        }
        InternalKnowledgeBase kbase = KnowledgeBaseFactory.newKnowledgeBase();
        Collection<KiePackage> pkgs = kbuilder.getKnowledgePackages();
        kbase.addPackages(pkgs);
        KieSession kieSession = kbase.newKieSession();
        Person person = new Person("1",21);
        Person person1 = new Person("1",18);
        kieSession.insert(person);
        kieSession.insert(person1);

        kieSession.fireAllRules();
        kieSession.dispose();
    }
    public static KieBase getKieBase(String classPath) throws Exception {
        KieServices kieServices = KieServices.Factory.get();
        KieFileSystem kfs = kieServices.newKieFileSystem();
        Resource resource = ResourceFactory.newFileResource(classPath);
        kfs.write(resource);
        KieBuilder kieBuilder = kieServices.newKieBuilder(kfs).buildAll();
        if (kieBuilder.getResults().getMessages(Message.Level.ERROR).size() > 0) {
            throw new Exception();
        }
        KieContainer kieContainer = kieServices.newKieContainer(kieServices.getRepository()
                .getDefaultReleaseId());
        return kieContainer.getKieBase();
    }

}
