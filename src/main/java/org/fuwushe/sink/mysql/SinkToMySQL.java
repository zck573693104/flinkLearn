package org.fuwushe.sink.mysql;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.fuwushe.item.Result;
import org.fuwushe.utils.HikariDatasource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;


public class SinkToMySQL extends RichSinkFunction<List<Result>> {

    private static HikariDataSource dataSource;

    private static Logger logger = LoggerFactory.getLogger(SinkToMySQL.class);

    private static final long serialVersionUID = -8972576467903242251L;

    private static PreparedStatement ps;

    private static Connection connection;

    private String behavior;

    private String pordOrTest;

    public SinkToMySQL(String behavior,String pordOrTest) {
        this.pordOrTest = pordOrTest;
        this.behavior = behavior;
    }

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
        dataSource = HikariDatasource.getDatasource(pordOrTest);
    }

    private void init() throws SQLException {

        connection = dataSource.getConnection();
        String sql = "insert into biz_da.biz_tbl_item_view (item_id, window_end_date, view_count,behavior) values(?, ?, ?,?);";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {

        logger.info("连接池初始化关闭");
        super.close();
        connectionClose();
    }

    private void connectionClose() throws SQLException {

        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(List<Result> value, Context context) throws Exception {
        init();
        //遍历数据集合
        for (Result result : value) {
            ps.setString(1, result.getItemId());
            ps.setString(2, result.getWindowEndDate());
            ps.setLong(3, result.getViewCount());
            ps.setString(4, behavior);
            ps.addBatch();
        }
        ps.executeBatch();//批量后执行
        connectionClose();
    }


}
