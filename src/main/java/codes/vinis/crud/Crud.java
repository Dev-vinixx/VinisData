package codes.vinis.crud;

import codes.vinis.authentication.MysqlAuthentication;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.Range;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public class Crud {

    @NotNull MysqlAuthentication mysqlAuthentication;

    public Crud(@Nullable String username, @Nullable String password, @NotNull InetAddress hostname, @Range(from = 0, to = 65535) int port) {
        mysqlAuthentication = new MysqlAuthentication(username, password, hostname, port);
    }

    /**
     * This method is used to asynchronously insert a new record into a specified table in a MySQL database.
     *
     * @param table  The name of the table in the database where the record will be inserted.
     *               This parameter cannot be null or empty.
     * @param values A HashMap containing the column names as keys and the corresponding values to be inserted.
     *               This parameter cannot be null or empty.
     *
     * @return A CompletableFuture that will be completed when the database operation is finished.
     *         If the operation is successful, the CompletableFuture is completed normally.
     *         If an exception occurs during the operation, the CompletableFuture is completed exceptionally with the exception.
     *
     * @throws IllegalArgumentException If the table name or values are null or empty.
     * @throws SQLException If a database access error occurs or the connection object is null.
     */
    @NotNull
    public final CompletableFuture<Void> createRecord(String table, HashMap<String, Object> values) {

        if (table == null || table.isEmpty()) {
            throw new IllegalArgumentException("table name cannot be null or empty");
        }
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("values cannot be null or empty");
        }

        @NotNull CompletableFuture<Void> future = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {
           try(@NotNull Connection connection = mysqlAuthentication.getConnection()) {

               if (connection == null) {
                   throw new SQLException("Connection is null");
               }

               @NotNull String columns = String.join(", ", values.keySet());
               @NotNull String placeholders = String.join(", ", Collections.nCopies(values.size(),"?"));

               @NotNull String sql = "INSERT INTO " + table + " (" + columns + ") VALUES (" + placeholders + ")";

               try(PreparedStatement preparedStatement = connection.prepareStatement(sql)) {

                   int index = 1;
                   for (Object value : values.values()) {
                       preparedStatement.setObject(index++, value);
                   }

                   preparedStatement.executeUpdate();
               }
           } catch (@NotNull Throwable throwable) {
               future.completeExceptionally(throwable);
           }
        });

        return future;
    }
}