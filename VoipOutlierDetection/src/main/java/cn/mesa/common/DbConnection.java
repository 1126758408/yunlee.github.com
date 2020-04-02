package cn.mesa.common;

import cn.mesa.utils.PropUtils;
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import scala.Serializable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
//import java.util.Properties;

public class DbConnection implements Serializable {
	private static DruidDataSource dataSource = null;
    private static DbConnection dbConnect = null;
    //private static Properties props = new Properties();

    static {
        getDbConnect();
    }

    private static void getDbConnect() {
        try {
            if (dataSource == null) {
                dataSource = new DruidDataSource();
                //props.load(DbConnection.class.getClassLoader().getResourceAsStream("clickhouse.properties"));
                //设置连接参数
                dataSource.setUrl(PropUtils.writeDBUrl());
                dataSource.setDriverClassName(PropUtils.DBDriver());
                dataSource.setUsername(PropUtils.DBUser());
                dataSource.setPassword(PropUtils.DBPassword());
                //配置初始化大小、最小、最大
                dataSource.setInitialSize(PropUtils.initial());
                dataSource.setMinIdle(PropUtils.minidle());
                dataSource.setMaxActive(PropUtils.maxactive());
//                dataSource.setUrl(props.getProperty("writedb.url"));
//                dataSource.setDriverClassName(props.getProperty("drivers"));
//                dataSource.setUsername(props.getProperty("mdb.user"));
//                dataSource.setPassword(props.getProperty("mdb.password"));
//                //配置初始化大小、最小、最大
//                dataSource.setInitialSize(Integer.parseInt(props.getProperty("initialsize")));
//                dataSource.setMinIdle(Integer.parseInt(props.getProperty("minidle")));
//                dataSource.setMaxActive(Integer.parseInt(props.getProperty("maxactive")));
                //连接泄漏监测
                dataSource.setRemoveAbandoned(true);
                dataSource.setRemoveAbandonedTimeout(999999999);
                dataSource.setDefaultAutoCommit(false);
                //配置获取连接等待超时的时间
                dataSource.setMaxWait(999999999);
                //配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒
                dataSource.setTimeBetweenEvictionRunsMillis(999999999);
                //防止过期
                dataSource.setValidationQuery("SELECT 1");
                dataSource.setTestWhileIdle(true);
                dataSource.setTestOnBorrow(true);
                dataSource.setKeepAlive(true);
            }
        } catch (Exception e) {
            e.printStackTrace();

        }
    }

    /**
     * 数据库连接池单例
     *
     * @return dbConnect
     */
    public static synchronized DbConnection getInstance() {
        if (null == dbConnect) {
            dbConnect = new DbConnection();
        }
        return dbConnect;
    }

    /**
     * 返回druid数据库连接
     *
     * @return 连接
     * @throws SQLException sql异常
     */
    public DruidPooledConnection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    /**
     * 清空PreparedStatement、Connection对象，未定义的置空。
     *
     * @param pstmt      PreparedStatement对象
     * @param connection Connection对象
     */
    public void clear(PreparedStatement pstmt, Connection connection) {
        try {
            if (pstmt != null) {
                pstmt.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
}
