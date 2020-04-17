package com.flink.sink.mysql;

import com.flink.utils.HikariDatasource;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import com.flink.item.Result;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SinkUserPvToMySQL2 extends RichSinkFunction<Result> {

    private static final long serialVersionUID = 4224636085166061075L;

    private static HikariDataSource dataSource;

    private static PreparedStatement ps;

    private static Connection connection;

    private String behavior;

    private String pordOrTest;

    public SinkUserPvToMySQL2(String behavior,String pordOrTest) {

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

        super.open(parameters);
        dataSource = HikariDatasource.getDatasource(pordOrTest);
    }

    @Override
    public void close() throws Exception {
        super.close();
        connectionClose();
    }

    private void connectionClose() throws SQLException {
        if (ps != null) {
            ps.close();
        }
        if (connection != null) {
            connection.close();
        }

    }

    private void init() throws SQLException {

        connection = dataSource.getConnection();
        String sql = "insert into biz_da.biz_tbl_item_view (item_id, window_end_date, view_count,behavior) values(?, ?, ?,?);";
        ps = connection.prepareStatement(sql);
    }

    @Override
    public void invoke(Result result, Context context) throws Exception {
        init();
        ps.setString(1, result.getItemId());
        ps.setString(2, result.getWindowEndDate());
        ps.setLong(3, result.getViewCount());
        ps.setString(4, behavior);
        ps.executeUpdate();
        connectionClose();
    }

}