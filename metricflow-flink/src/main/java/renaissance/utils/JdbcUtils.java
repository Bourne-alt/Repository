package renaissance.utils;



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcUtils {
    private static Connection conn=null;
    public static Connection getConn(String databasename) throws SQLException {
        conn = DriverManager.getConnection(
                "jdbc:mysql://master1:3306/"+databasename+"?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&autoReconnect=true"
                , "root"
                , "Bdpp1234!"
        );
        return conn;
    }
}
