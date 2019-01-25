package org.apache.pinot.thirdeye.datasource.presto;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.tomcat.jdbc.pool.DataSource;


public class PrestoClient {
  private static volatile PrestoClient _instance = null;

  private DataSource dataSource;

  public PrestoClient(String jdbcUrl, String user, String password) {

    dataSource = new DataSource();
//    dataSource.setDriverClassName("com.facebook.presto");
    dataSource.setInitialSize(10);
    dataSource.setMaxActive(20);
    dataSource.setUsername(user);
    dataSource.setPassword(password);
    dataSource.setUrl(jdbcUrl);

    dataSource.setValidationQuery("select 1");
    dataSource.setTestWhileIdle(true);
    dataSource.setTestOnBorrow(true);
    // when returning connection to pool
    dataSource.setTestOnReturn(true);
    dataSource.setRollbackOnReturn(true);

    // Timeout before an abandoned(in use) connection can be removed.
    dataSource.setRemoveAbandonedTimeout(600_000);
    dataSource.setRemoveAbandoned(true);

  }


  public Connection getConnection() {
    Connection conn = null;
    try {
      conn = dataSource.getConnection();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(0);
    }
    return conn;
  }
}
