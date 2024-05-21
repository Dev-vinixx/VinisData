package codes.vinis.crud;

import codes.vinis.authentication.MysqlAuthentication;
import codes.vinis.crud.util.Column;
import codes.vinis.crud.util.Table;
import codes.vinis.database.Database;
import org.jetbrains.annotations.*;

import java.net.InetAddress;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

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
            try {
                @Nullable Connection connection;

                if (mysqlAuthentication.getConnection() != null) {
                    connection = mysqlAuthentication.getConnection();
                } else {
                    connection = mysqlAuthentication.connect().join();
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
     * Asynchronously performs a query to read data from a database table.
     *
     * @param database   The database to query.
     * @param table      The table from which data will be read.
     * @param columns    The specific columns to select in the query.
     * @param condition  The conditions to filter the results (column and values).
     * @return A CompletableFuture containing a list of maps, where each map represents a data row.
     *         Each map's key is the column name, and the value is the corresponding value.
     * @throws SQLException if an error occurs during query execution.
     */
    @NotNull
    public final CompletableFuture<List<Map<String, Object>>> read(@NotNull Database database, @NotNull Table table, @NotNull List<Column> columns, @NotNull Map<Column, List<@Nullable Object>> condition) {
        @NotNull CompletableFuture<List<Map<String, Object>>> future = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {
            try {
                @Nullable Connection connection;

                if (mysqlAuthentication.getConnection() != null) {
                    connection = mysqlAuthentication.getConnection();
                } else {
                    connection = mysqlAuthentication.connect().join();
                }

                @NotNull String setCause = columns.stream().map(Column::toString).collect(Collectors.joining(", "));
                @NotNull String setConditions = condition.entrySet().stream()
                        .map(entry -> entry.getKey() + " IN (" + entry.getValue().stream().map(v -> "?").collect(Collectors.joining(", ")) + ")")
                        .collect(Collectors.joining(" AND "));
                @NotNull String sql = "SELECT " + setCause + " FROM " + database.toString() + "." + table.toString() + " WHERE " + setConditions;

                try (@NotNull PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                    int index = 1;
                    for (List<Object> values : condition.values()) {
                        for (Object value : values) {
                            preparedStatement.setObject(index++, value);
                        }
                    }

                    try (@NotNull ResultSet resultSet = preparedStatement.executeQuery()) {
                        ResultSetMetaData metaData = resultSet.getMetaData();
                        int columnCount = metaData.getColumnCount();

                        List<Map<String, Object>> results = new ArrayList<>();
                        while (resultSet.next()) {
                            Map<String, Object> row = new HashMap<>();
                            for (int i = 1; i <= columnCount; i++) {
                                row.put(metaData.getColumnName(i), resultSet.getObject(i));
                            }
                            results.add(row);
                        }
                        future.complete(results);
                    }
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
            try {
                @Nullable Connection connection;

                if (mysqlAuthentication.getConnection() != null) {
                    connection = mysqlAuthentication.getConnection();
                } else {
                    connection = mysqlAuthentication.connect().join();
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
            try {
                @Nullable Connection connection;

                if (mysqlAuthentication.getConnection() != null) {
                    connection = mysqlAuthentication.getConnection();
                } else {
                    connection = mysqlAuthentication.connect().join();
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
