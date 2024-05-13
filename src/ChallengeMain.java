import com.mysql.cj.jdbc.MysqlDataSource;
import com.mysql.cj.jdbc.exceptions.MysqlDataTruncation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.List;

public class ChallengeMain {
    public static String insertOrder = "INSERT INTO storefront.order (order_date) VALUES (?)";
    public static String insertOrderItem = "INSERT INTO storefront.order_details (item_description, order_id, quantity) VALUES (?, ?, ?)";

    public static void main(String[] args) {
        var dataSource = new MysqlDataSource();
        dataSource.setPort(3306);
        dataSource.setServerName("localHost");
        dataSource.setDatabaseName("storefront");

        try {
            dataSource.setContinueBatchOnError(false);
        } catch(SQLException e) {
            e.printStackTrace();
        }

        //reading the .csv, if length = 2, then new order
        try (Connection conn = dataSource.getConnection(
                System.getenv("MYSQLUSER"),System.getenv("MYSQLPASS"))
//             ;Statement runOnce = conn.createStatement()
        ){
//             runOnce.execute("ALTER TABLE storefront.order_details ADD COLUMN quantity int");
            addFromCsv(conn);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


    }

    public static int addOrder(PreparedStatement ps, String date) throws SQLException {
        int orderId = -1;
        ps.setString(1, date);
        try {
            int results = ps.executeUpdate();
            if(results > 0) {
                ResultSet rs = ps.getGeneratedKeys();
                if(rs.next()) {
                    orderId = rs.getInt(1);
                    System.out.println("Found new orderId: " + orderId);
                }
            }
        } catch(MysqlDataTruncation e) {
            throw new RuntimeException(e);
        }

        return orderId;
    }
    public static void addOrderItem(PreparedStatement ps, int orderId,
                                   String item, int quantity) throws SQLException {
//        int orderId = -1;
        ps.setString(1, item);
        ps.setInt(2, orderId);
        ps.setInt(3, quantity);
        ps.addBatch();

//        return orderId;
    }

    public static void addFromCsv(Connection conn) throws SQLException {
        List<String> reads = null;
        try {
            reads = Files.readAllLines(Path.of("Orders.csv"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (PreparedStatement psOrder = conn.prepareStatement(insertOrder, Statement.RETURN_GENERATED_KEYS);
             PreparedStatement psItem = conn.prepareStatement(insertOrderItem, Statement.RETURN_GENERATED_KEYS)
                ){
            conn.setAutoCommit(false);
            int orderId = -1;
            for(String row : reads) {
                String[] cols = row.split(",");
                if(cols[0].equals("order")) {
                    System.out.println("Setting new order..");
                    if(orderId != -1) {
                        isolatedBatchUpload(psItem, conn);
                    }
                    orderId = addOrder(psOrder, cols[1]);
//                    continue; //just for the current iteration of the for:each
                } else {
                    addOrderItem(psItem, orderId, cols[2], Integer.parseInt(cols[1]));
                }
            }
            isolatedBatchUpload(psItem, conn);
            conn.commit();
        } catch (SQLException e) {
            conn.rollback();
            throw new RuntimeException(e);
        } finally {
            conn.setAutoCommit(true);
        }

    }

    public static void isolatedBatchUpload(PreparedStatement ps, Connection conn)
    throws SQLException {
        try {
            ps.executeBatch();
            conn.commit();
            System.out.println("Yummy");
        } catch (SQLException e) {
            conn.rollback();
            throw new RuntimeException(e);
        }
    }
}
