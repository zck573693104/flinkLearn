package org.fuwushe.jdbc;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MysqlClient {

    private static String  driverClass = "com.mysql.cj.jdbc.Driver";
    private static String dbUrl = "jdbc:mysql://localhost:3306/flink?autoReconnect=true&failOverReadOnly=false&useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8";
    private static String userNmae = "root";
    private static String passWord = "123456";
    private static java.sql.Connection conn;
    private static PreparedStatement ps;

    static {
        try {
            Class.forName(driverClass);
            conn = DriverManager.getConnection(dbUrl, userNmae, passWord);
            ps = conn.prepareStatement("select phone from student where id = ?");
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * execute query
     *
     * @param user
     * @return
     */
    public User query1(User user) {



        String phone = "0000";
        try {
            ps.setString(1, user.getId());
            ResultSet rs = ps.executeQuery();
            if (!rs.isClosed() && rs.next()) {
                phone = rs.getString(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        user.setPhone(phone);
        return user;

    }


}