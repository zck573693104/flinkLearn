package com.flink.sink.mysql;

import com.flink.config.MybatisSessionFactory;
import com.flink.demo.OrderEvent;
import com.flink.sink.mapper.TestMapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.ibatis.session.SqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SinkMybatis<IN> extends RichSinkFunction<Tuple2<String,IN>> {


    private static Logger logger = LoggerFactory.getLogger(SinkMybatis.class);

    private static final long serialVersionUID = -8972576467903242251L;

    private SqlSession sqlSession;



    /**一个并行度一次 一个task一个connection
     * open() 连接池初始化
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {

        logger.info("连接池初始化");
        super.open(parameters);
        sqlSession = MybatisSessionFactory.getSqlSessionFactory().openSession();
    }


    @Override
    public void close() throws Exception {

        logger.info("连接池初始化关闭");
        super.close();
    }


    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param in
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(Tuple2<String,IN> in, Context context) throws Exception {
        String type = in.f0;
        OrderEvent orderEvent = (OrderEvent)in.f1;
        TestMapper testMapper = sqlSession.getMapper(TestMapper.class);
        testMapper.save("是我");
        sqlSession.commit();

    }


}
