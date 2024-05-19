package codes.vinis.crud;

import codes.vinis.authentication.MysqlAuthentication;
import codes.vinis.crud.util.Column;
import codes.vinis.crud.util.Table;
import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Range;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.sql.PreparedStatement;


public class Crud {

    private final @Nullable MysqlAuthentication mysqlAuthentication;

    public Crud(@Nullable String username, @Nullable String password, @NotNull InetAddress hostname, @Range(from = 0, to = 65535) int port) {
        mysqlAuthentication = new MysqlAuthentication( username, password, hostname, port);
    };

    /**
     * Asynchronous method to insert a new record into a specific table.
     *
     * @param table  The table in which the new record will be inserted.
     * @param values A HashMap mapping each column (key) to a corresponding value (value).
     * The keys are of type Column and the values are of type Object, which can be null (@Nullable).
     *
     * @throws SQLException If the connection is null or if there is any error during the execution of the SQL statement.
     *
     * @Blocking This method is blocking as it waits for the completion of the insert operation.
     *
     * @NotNull This method neither accepts nor returns null values, except the values in the HashMap.
     */
    @Blocking
    public void create(Table table, HashMap<Column, @Nullable Object> values) throws SQLException {

        @NotNull CompletableFuture<Connection> future = mysqlAuthentication.connect();

        CompletableFuture.runAsync(() -> {

            try (@NotNull Connection connection = future.join()) {

                if (connection == null) {
                    throw new SQLException("connection is null");
                }

                @NotNull String columns = values.keySet().stream().map(Column::toString).collect(Collectors.joining(", "));
                @NotNull String placeholders = String.join(", ", Collections.nCopies((values.size()), "?"));

                @NotNull String sql = "INSERT INTO " + "test_foda . " + table.toString() + " (" + columns +") VALUES (" + placeholders + ")";

                try(PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                    int index = 1;
                    for ( @NotNull Object value : values.values()) {
                        preparedStatement.setObject(index++, value);
                    }

                    preparedStatement.executeUpdate();
                }

            } catch (@NotNull Throwable throwable) {
                future.completeExceptionally(throwable);
            }
        });

    }

}
