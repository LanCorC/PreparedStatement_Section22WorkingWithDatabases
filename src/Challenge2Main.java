import com.mysql.cj.jdbc.MysqlDataSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

//Challenge for revisiting storefront schema with CallableStatement
public class Challenge2Main {
    public record Order (String date, List<OrderItems> items) {
        public Order(String date) {
            this(date, new ArrayList<>());
        }
    }
    public record OrderItems (String desc, int qty) { }

    public static void main(String[] args) {
        List<String> reads = null;
        //Read from Orders.csv
        try {
            reads = Files.readAllLines(Path.of("Orders.csv"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        //Process results
            //See: Map<Order date string, JSON string>
        List<Order> orders = new ArrayList<>();
        for(String line : reads) {
            String[] input = line.split(",");
            if(input[0].equals("order")) {
                orders.add(new Order(input[1]));
            } else {
                orders.get(orders.size()-1).items().add(
                        new OrderItems(
                                input[2], Integer.parseInt(input[1]))
                );
            }
        }
        //process into JSON string
        HashMap<Order, String> orderJSON = new HashMap<>();
        for(Order o : orders) {
            StringBuilder sb = new StringBuilder();
            sb.append("["); //suffix
            boolean first = true;
            for(OrderItems item : o.items()) {
                if(!first) {
                    sb.append(",");
                }
                first = false;
                sb.append("{\"itemDescription\":\"");
                sb.append(item.desc);
                sb.append("\", \"qty\":");
                sb.append(item.qty);
                sb.append("}");
            }
            sb.append("]"); //prefix
            orderJSON.put(o, sb.toString());
            System.out.println(sb);
        }

        var dataSource = new MysqlDataSource();

        dataSource.setServerName("localHost");
        dataSource.setPort(3306);
        dataSource.setDatabaseName("storefront");

        try(Connection connection = dataSource.getConnection(
                System.getenv("MYSQLUSER"), System.getenv("MYSQLPASS"))
        ) {
            CallableStatement cs = connection.prepareCall(
                    "{CALL storefront.addOrder(?,?,?,?)}");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("G yyyy-MM-dd HH:mm:ss")
                    .withResolverStyle(ResolverStyle.STRICT);
            orderJSON.forEach( (k, v) -> {
                try {
                    Timestamp ts = Timestamp.valueOf(LocalDateTime.parse("AD " + k.date(), formatter));
                    cs.setTimestamp(1, ts);
                    cs.setString(2, v);
                    cs.setInt(3, 0);
                    cs.setInt(4, 0);
                    cs.execute();
                    System.out.println("Added orderId: " + cs.getInt(3));
                    System.out.println("Records added: " + cs.getInt(4));
                } catch (Exception e) {
                    System.err.println("Something went wrong!" + e.getMessage());
                    e.printStackTrace();
                }
            });
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
