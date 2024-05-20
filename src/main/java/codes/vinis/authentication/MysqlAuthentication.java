package codes.vinis.authentication;

import org.jetbrains.annotations.*;

import java.net.InetAddress;
import java.sql.*;
import java.util.concurrent.*;

public class MysqlAuthentication {

    private final @Nullable String username;
    private final @Nullable String password;
    private final @NotNull InetAddress hostname;

    @Range(from = 0, to = 65535)
    private final int port;

    @ApiStatus.Internal
    private @Nullable Connection connection;

    private final @NotNull ExecutorService executorService = Executors.newCachedThreadPool();

    public MysqlAuthentication(@Nullable String username, @Nullable String password, @NotNull InetAddress hostname, @Range(from = 0, to = 65535) int port) {
        this.username = username;
        this.password = password;
        this.hostname = hostname;
        this.port = port;
    }

    public final @Nullable Connection getConnection() {
        return connection;
    }

    public final boolean isConnected() {
        return connection != null;
    }

    @Contract(pure = true)
    public final @Nullable String getUsername() {
        return username;
    }

    @Contract(pure = true)
    public final @Nullable String getPassword() {
        return password;
    }

    @Contract(pure = true)
    public final @NotNull InetAddress getHostname() {
        return hostname;
    }

    @Contract(pure = true)
    @Range(from = 0, to = 65535)
    public final int getPort() {
        return port;
    }

    /**
     * Attempts to load the JDBC driver for MySQL.
     *
     * @return The class of the JDBC driver for MySQL.
     * @throws RuntimeException If the JDBC driver cannot be loaded.
     */
    public @NotNull Class<Driver> getDriver() {
        try {
            try {
                return (Class<Driver>) Class.forName("com.mysql.cj.jdbc.Driver");
            } catch (ClassNotFoundException e) {
                return (Class<Driver>) Class.forName("com.mysql.jdbc.Driver");
            }
        } catch (Throwable e) {
            throw new RuntimeException("Cannot load default driver 'com.mysql.jdbc.Driver'", e);
        }
    }

    /**
     * Establishes a connection to the database asynchronously.
     *
     * @return A CompletableFuture that, when completed, will provide the established connection.
     * @throws IllegalStateException If a connection is already established.
     */
    public final @NotNull CompletableFuture<Connection> connect() {
        if (isConnected()) {
            throw new IllegalStateException("this authentication is already connected!");
        }

        @NotNull CompletableFuture<Connection> future = new CompletableFuture<>();

         CompletableFuture.runAsync(() -> {

            try {

                Class<Driver> ignore = getDriver();
                this.connection = load().get(10, TimeUnit.SECONDS);
                future.complete(connection);

            } catch (Throwable throwable) {
                try {

                    if (isConnected()) {
                        disconnect().get(3, TimeUnit.SECONDS);
                    }

                } catch(@NotNull Throwable throwable1) {
                    future.completeExceptionally(throwable1);
                }

                throw new CompletionException(throwable);
            }
        }, executorService);

         return future;
    }

    /**
     * Sets up and establishes a connection to the database asynchronously.
     *
     * @return A CompletableFuture that, when completed, will provide the established connection.
     * @throws CompletionException If an error occurs while establishing the connection.
     */
    @ApiStatus.OverrideOnly
    public @NotNull CompletableFuture<Connection> load() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                @NotNull Connection connection = DriverManager.getConnection(
                        "jdbc:mysql://" + getHostname().getHostAddress() + ":" + getPort() +
                                "/?autoReconnect=true&failOverReadOnly=false&verifyServerCertificate=false",
                        getUsername(), getPassword()
                );
                connection.setNetworkTimeout(executorService, (int) TimeUnit.MINUTES.toMillis(30));
                return connection;
            } catch (SQLException e) {
                throw new CompletionException("Error loading the database connection", e);
            }
        }, executorService);
    }

    /**
     * Disconnects from the database asynchronously.
     *
     * @return A CompletableFuture that represents the asynchronous disconnect operation.
     * @throws IllegalStateException If the database is not currently connected.
     */
    public final @NotNull CompletableFuture<Void> disconnect() {
        if (!isConnected()) {
            throw new IllegalStateException("This authentication isn't connected");
        }

        return CompletableFuture.runAsync(() -> {
            try {
                if (connection != null) {
                    connection.close();
                    connection = null;
                }
            } catch (SQLException e) {
                throw new CompletionException("Error closing the connection", e);
            }
        }, executorService);
    }

    /**
     * Reconnects to the database asynchronously. If a connection is already established, it disconnects first.
     * Both the disconnect and connect operations are subject to a timeout of 5 seconds.
     *
     * @return A CompletableFuture that represents the asynchronous reconnect operation.
     * @throws CompletionException If an error occurs during reconnection.
     */
    @Blocking
    public final @NotNull CompletableFuture<Void> reconnect() {
        return CompletableFuture.runAsync(() -> {
            try {
                if (isConnected()) {
                    disconnect().get(5, TimeUnit.SECONDS);
                }
                connect().get(5, TimeUnit.SECONDS);
            } catch (Throwable throwable) {
                throw new CompletionException(throwable);
            }
        }, executorService);
    }

    /**
     * Shuts down the executor service, attempting a graceful shutdown.
     * This method should be called to clean up resources when the application is shutting down.
     */
    public void shutdownExecutor() {
        executorService.shutdown();

        try {

            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }

        } catch (InterruptedException e) {

            executorService.shutdownNow();
            Thread.currentThread().interrupt();

        }
    }
}