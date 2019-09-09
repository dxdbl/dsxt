package io.transwarp.udf;

import java.sql.*;

public class utils {
    //Hive2 Driver class name
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    //Hive2 JDBC URL with LDAP
    static final String jdbcURL = "jdbc:hive2://10.11.220.12:10010/dsxt";
    static final String user = "hive";
    static final String password = "123456";

    public static String getValue(String sql) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection conn = DriverManager.getConnection(jdbcURL, user, password);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        ResultSetMetaData rsmd = rs.getMetaData();
        int size = rsmd.getColumnCount();
        StringBuffer value = new StringBuffer();
        while(rs.next()) {
            for(int i = 0; i < size; i++) {
                value.append(rs.getString(i+1));
            }
        }
        rs.close();
        return value.toString();
    }
    public static void insert(String sql) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        Connection conn = DriverManager.getConnection(jdbcURL, user, password);
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
    }
}