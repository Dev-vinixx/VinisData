package codes.vinis.crud;

import codes.vinis.database.Database;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Range;

public class Column extends Database {

    public Column(@NotNull String name, @Range(from = 0, to = Long.MAX_VALUE) long id) {
        super(name, id);
    }

}
