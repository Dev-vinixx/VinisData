package authentication;

import org.jetbrains.annotations.*;

import java.net.InetAddress;
import java.sql.Connection;

public class MysqlAuthentication {

    private final @Nullable String username;
    private final @Nullable String password;

    private final @NotNull InetAddress hostname;

    @Range(from = 0, to = 65535)
    private final int port;

    @ApiStatus.Internal
    private @Nullable Connection connection;

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


}