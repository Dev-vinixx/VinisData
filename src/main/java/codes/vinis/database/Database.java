package codes.vinis.database;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Range;

public class Database {

    private final @NotNull String name;
    private final @Range(from = 0, to = Long.MAX_VALUE) long id;

    public Database(@NotNull String name, @Range(from = 0, to = Long.MAX_VALUE) long id) {
        this.name = name;
        this.id = id;
    }

    @Override
    public String toString() {
        return name;
    }

    @Range(from = 0, to = Long.MAX_VALUE)
    public long getId() {
        return id;
    }
}
