package org.fuwushe.jdbc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class AsyncFunctionForMysqlJava extends RichAsyncFunction<User, AsyncUser> {


    Logger logger = LoggerFactory.getLogger(AsyncFunctionForMysqlJava.class);
    private transient MysqlClient client;
    private transient ExecutorService executorService;

    /**
     * open 方法中初始化链接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        logger.info("async function for mysql java open ...");
        super.open(parameters);

        client = new MysqlClient();
        executorService = Executors.newFixedThreadPool(10);
    }

    /**
     * use asyncUser.getId async get asyncUser phone
     *
     * @param asyncUser
     * @param resultFuture
     * @throws Exception
     */
    @Override
    public void asyncInvoke(User asyncUser, ResultFuture<AsyncUser> resultFuture)  {

        executorService.submit(() -> {
            try {
                User user = client.query1(asyncUser);
                AsyncUser tmp  = new AsyncUser();
                tmp.setId(user.getId());
                tmp.setPhone(user.getPhone());
                resultFuture.complete(Collections.singletonList(tmp));
            } catch (Exception e) {
                resultFuture.complete(new ArrayList<>(0));
            }


        });
    }

    @Override
    public void timeout(User input, ResultFuture<AsyncUser> resultFuture) throws Exception {
        List<AsyncUser> list = new ArrayList();
        AsyncUser asyncUser = new AsyncUser();
        asyncUser.setPhone("timeout");
        list.add(asyncUser);
        resultFuture.complete(list);
    }

    /**
     * close function
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        logger.info("async function for mysql java close ...");
        super.close();
    }
}