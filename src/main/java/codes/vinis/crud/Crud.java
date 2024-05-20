package codes.vinis.crud;

import codes.vinis.authentication.MysqlAuthentication;
import codes.vinis.crud.util.Column;
import codes.vinis.crud.util.Table;
import codes.vinis.database.Database;
import org.jetbrains.annotations.*;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.sql.PreparedStatement;

public class Crud {

    private final @Nullable MysqlAuthentication mysqlAuthentication;

    public Crud(@Nullable String username, @Nullable String password, @NotNull InetAddress hostname, @Range(from = 0, to = 65535) int port) {
        this.mysqlAuthentication = new MysqlAuthentication(username, password, hostname, port);
    }

    /**
     * Asynchronous method to insert a new record into a specific table.
     *
     * @param database The database in which the new record will be inserted.
     * @param table    The table in which the new record will be inserted.
     * @param values   A HashMap mapping each column to a corresponding value.
     * @return A CompletableFuture that completes when the insertion is done.
     */
    @NotNull
    public final CompletableFuture<Void> create(@NotNull Database database, @NotNull Table table, @NotNull HashMap<Column, @Nullable Object> values) {

        @NotNull CompletableFuture<Void> future = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {
            try (@NotNull Connection connection = mysqlAuthentication.connect().join()) {

                if (connection == null) {
                    throw new SQLException("connection is null");
                }

                String columns = values.keySet().stream().map(Column::toString).collect(Collectors.joining(", "));
                String placeholders = values.keySet().stream().map(k -> "?").collect(Collectors.joining(", "));

                String sql = "INSERT INTO " + database + "." + table + " (" + columns + ") VALUES (" + placeholders + ")";

                try (@NotNull PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                    int index = 1;
                    for (Object value : values.values()) {
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
     * Asynchronous method to update a record in a specific table.
     *
     * @param database  The database containing the table.
     * @param table     The table to update.
     * @param values    A HashMap of columns and their new values.
     * @param condition A HashMap of columns and their values to identify which rows to update.
     * @return A CompletableFuture that completes when the update is done.
     */
    @NotNull
    public final CompletableFuture<Void> update(@NotNull Database database, @NotNull Table table, @NotNull HashMap<Column, @Nullable Object> values, @NotNull HashMap<Column, @Nullable Object> condition) {

        @NotNull CompletableFuture<Void> future = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {
            try (@NotNull Connection connection = mysqlAuthentication.connect().join()) {

                if (connection == null) {
                    throw new SQLException("connection is null");
                }

                String setClause = values.keySet().stream()
                        .map(column -> column + " = ?")
                        .collect(Collectors.joining(", "));

                String whereClause = condition.keySet().stream()
                        .map(column -> column + " = ?")
                        .collect(Collectors.joining(" AND "));

                String sql = "UPDATE " + database + "." + table + " SET " + setClause + " WHERE " + whereClause;

                try (@NotNull PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                    int index = 1;

                    for (Object value : values.values()) {
                        preparedStatement.setObject(index++, value);
                    }

                    for (Object value : condition.values()) {
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
     * Asynchronous method to delete rows from a specific table.
     *
     * @param database  The database containing the table.
     * @param table     The table to delete from.
     * @param condition A HashMap of columns and their values to identify which rows to delete.
     * @return A CompletableFuture that completes when the deletion is done.
     */
    @NotNull
    public final CompletableFuture<Void> delete(@NotNull Database database, @NotNull Table table, @NotNull HashMap<Column, @Nullable Object> condition) {

        @NotNull CompletableFuture<Void> future = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {
            try (@NotNull Connection connection = mysqlAuthentication.connect().join()) {

                if (connection == null) {
                    throw new SQLException("connection is null");
                }

                String whereClause = condition.keySet().stream()
                        .map(column -> column + " = ?")
                        .collect(Collectors.joining(" AND "));

                String sql = "DELETE FROM " + database + "." + table + " WHERE " + whereClause;

                try (@NotNull PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                    int index = 1;

                    for (Object value : condition.values()) {
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
}
