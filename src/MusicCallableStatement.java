import com.mysql.cj.jdbc.MysqlDataSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.Map;
import java.util.stream.Collectors;

public class MusicCallableStatement {

    private static final int ARTIST_COLUMN = 0;
    public static final int ALBUM_COLUMN = 1;
    public static final int SONG_COLUMN = 3;
    public static void main(String[] args) {
        Map<String, Map<String, String>> albums = null;

        try(var lines = Files.lines(Path.of("NewAlbums.csv"))) {
            albums = lines.map(s -> s.split(","))
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

        }

        albums.forEach((artist, artistAlbums) -> {
            artistAlbums.forEach((key, value) -> {
                System.out.println(key + " : " + value);
            });
        });

        var dataSource = new MysqlDataSource();

        dataSource.setServerName("localHost");
        dataSource.setPort(3306);
        dataSource.setDatabaseName("music");

        try(Connection connection = dataSource.getConnection(
                System.getenv("MYSQLUSER"), System.getenv("MYSQLPASS"))
        ) {
            CallableStatement cs = connection.prepareCall(
                    "CALL music.addAlbumInOutCounts(?,?,?,?)");
            albums.forEach((artist, albumMap) -> {
                albumMap.forEach((album, songs) -> {
                    try {
                        cs.setString(1, artist);
                        cs.setString(2, album);
                        cs.setString(3, songs);
                        cs.setInt(4,10);
                        cs.registerOutParameter(4, Types.INTEGER);
                        cs.execute();
                        System.out.printf("%d songs were added for %s%n",
                                cs.getInt(4), album);
                    } catch (SQLException e) {
                        System.err.println(e.getErrorCode()+ " " + e.getMessage());
                    }
                });
            });

            String sql = "SELECT * FROM music.albumview WHERE artist_name = ?";
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.setString(1, "Bob Dylan");
            ResultSet rs = ps.executeQuery();
            Main.printRecords(rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
