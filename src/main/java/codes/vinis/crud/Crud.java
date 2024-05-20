package codes.vinis.crud;

import codes.vinis.authentication.MysqlAuthentication;
import codes.vinis.crud.util.Column;
import codes.vinis.crud.util.Table;
import codes.vinis.database.Database;
import org.jetbrains.annotations.*;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
    @NotNull
    public final CompletableFuture<Void> create(Database database,Table table, HashMap<Column, @Nullable Object> values) throws SQLException {

        @NotNull CompletableFuture<Void> future = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {

            try (@NotNull Connection connection = mysqlAuthentication.connect().join()) {

                if (connection == null) {
                    throw new SQLException("connection is null");
                }

                @NotNull String columns = values.keySet().stream().map(Column::toString).collect(Collectors.joining(", "));
                @NotNull String placeholders = String.join(", ", Collections.nCopies((values.size()), "?"));

                @NotNull String sql = "INSERT INTO " + database.toString() +" . " + table.toString() + " (" + columns +") VALUES (" + placeholders + ")";

                try(PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                    int index = 1;
                    for ( Object value : values.values()) {
                        preparedStatement.setObject(index++, value);
                    }

                    preparedStatement.executeUpdate();

                    future.complete(null);
                }

            } catch (@NotNull Throwable throwable) {
                future.completeExceptionally(throwable);
            }
        });
        return future;
    }

    /**
     * Asynchronous method to delete rows from a specific table in a specific database that match the given conditions.
     *
     * @param database   The database from which rows will be deleted. Must not be null.
     * @param table      The table from which rows will be deleted. Must not be null.
     * @param conditions A HashMap mapping each column (key) to a corresponding value (value). The keys are of type Column and the values are of type Object, which can be null (@Nullable).
     * @return A CompletableFuture that completes when the deletion is done, or exceptionally if an error occurs.
     *
     * @throws SQLException If the connection is null or if there is any error during the execution of the SQL statement.
     *
     * @Blocking This method is blocking as it waits for the completion of the delete operation.
     *
     * @NotNull This method does not return null values. The parameters database, table, and conditions must not be null, but the values in the conditions HashMap can be null.
     */
    @Blocking
    @NotNull
    public CompletableFuture<Void> delete(Database database, Table table, HashMap<Column, @Nullable Object> conditions) {

        @NotNull CompletableFuture<Void> future = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {

            @NonNls @NotNull StringBuilder sql = new StringBuilder("DELETE FROM " + database.toString() + " . " + table.toString() + " WHERE ");

            for(Map.Entry<Column, Object> entry : conditions.entrySet()) {
                sql.append(entry.getKey().toString()).append(" = ? AND ");
            }

            sql.setLength(sql.length() - 5);

            try(Connection connection = mysqlAuthentication.connect().join()) {

                if (connection == null) {
                    throw new SQLException("connection is null");
                }

                @NotNull PreparedStatement preparedStatement = connection.prepareStatement(sql.toString());

                int index = 1;

                for(Map.Entry<Column, Object> entry : conditions.entrySet()) {
                    preparedStatement.setObject(index++, entry.getValue());
                }

                preparedStatement.executeUpdate();

                future.complete(null);
            } catch(@NotNull Throwable throwable) {
                future.completeExceptionally(throwable);
            }
        });

        return future;
    }
}