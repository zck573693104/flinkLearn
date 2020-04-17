package org.fuwushe.drools;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.drools.core.impl.InternalKnowledgeBase;
import org.drools.core.impl.KnowledgeBaseFactory;
import org.fuwushe.kerberos.Kerberos;
import org.kie.api.definition.KiePackage;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.internal.builder.KnowledgeBuilder;
import org.kie.internal.builder.KnowledgeBuilderError;
import org.kie.internal.builder.KnowledgeBuilderErrors;
import org.kie.internal.builder.KnowledgeBuilderFactory;
import org.kie.internal.io.ResourceFactory;

import java.util.Collection;

public class DroolsTest {
    private static String PROD_OR_TEST = "local";

    private static String TOPIC_NAME;

    private static final String PROD_TOPPIC_NAME = "prodOrderTopic";

    private static final String TEST_TOPPIC_NAME = "testOrderTopic";

    private static String GROUP_NAME = "flinkProdOrderGroup";

    public static void main(String[] args) throws Exception {

        if (args.length != 0) {
            Kerberos.getKerberosJaas();
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            PROD_OR_TEST = parameterTool.get("prod_or_test");
            TOPIC_NAME = parameterTool.get("topic_name");
            GROUP_NAME = parameterTool.get("group_name");
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        AfterMatchSkipStrategy skipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
        DataStream<Person> input = env.fromElements(new Person("1", 8), new Person("2", 10),
                new Person("3", 18),
                new Person("4", 15),
                new Person("5", 25));
        input.filter(new FilterFunction<Person>() {
            @Override
            public boolean filter(Person person) throws Exception {
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
                kieSession.insert(person);

                kieSession.fireAllRules();
                return person.whetherFilter;
            }
        }).print();
//        input.map(new RichMapFunction<Person, Person>() {
//            @Override
//            public Person map(Person person) throws Exception {
//                KnowledgeBuilder kbuilder = KnowledgeBuilderFactory.newKnowledgeBuilder();
//                kbuilder.add(ResourceFactory.newClassPathResource("rules/Test.drl"), ResourceType.DRL);
//
//                KnowledgeBuilderErrors errors = kbuilder.getErrors();
//                if (errors.size() > 0) {
//                    for (KnowledgeBuilderError error: errors) {
//                        System.err.println(error);
//                    }
//                    throw new IllegalArgumentException("Could not parse knowledge.");
//                }
//                InternalKnowledgeBase kbase = KnowledgeBaseFactory.newKnowledgeBase();
//                Collection<KiePackage> pkgs = kbuilder.getKnowledgePackages();
//                kbase.addPackages(pkgs);
//                KieSession kieSession = kbase.newKieSession();
//                kieSession.insert(person);
//
//                kieSession.fireAllRules();
//                kieSession.dispose();
//                return person;
//            }
//        }).print();


        env.execute();
    }

}
