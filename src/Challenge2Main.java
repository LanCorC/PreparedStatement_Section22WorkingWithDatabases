import com.mysql.cj.jdbc.MysqlDataSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

//Challenge for revisiting storefront schema with CallableStatement
public class Challenge2Main {
    public static void main(String[] args) {
        List<String> orders = null;
        try(var lines = Files.lines(Path.of("Orders.csv"))) {
            orders = lines.map(s -> s.split(","))
                    .collect(Collectors.groupingBy(s -> s[ARTIST_COLUMN],
                            Collectors.groupingBy(s -> s[ALBUM_COLUMN],
                                    Collectors.mapping(s->s[SONG_COLUMN],
                                            Collectors.joining(
                                                    "\",\"",
                                                    "[\"",
                                                    "\"]"
                                            )))));
        } catch (IOException e) {
            throw new RuntimeException(e);
        } {

        var dataSource = new MysqlDataSource();

        dataSource.setServerName("localHost");
        dataSource.setPort(3306);
        dataSource.setDatabaseName("storefront");



        try(Connection connection = dataSource.getConnection(
                System.getenv("MYSQLUSER"), System.getenv("MYSQLPASS"))
        ) {

            LocalDate date = LocalDate.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");
            String time = date.format(formatter);
            Timestamp ts = Timestamp.valueOf(time);

            CallableStatement cs = connection.prepareCall(
                    "{CALL order.addOrder(?,?,?,?)}");
            cs.setTimestamp(1, ts);



        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
