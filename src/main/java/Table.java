public final class Table {
    private String keyspace;
    private String table;

    public Table(String keyspace, String table) {
        this.keyspace = keyspace;
        this.table = table;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public String getTable() {
        return table;
    }

    @Override
    public String toString() {
        return keyspace + '.' + table;
    }
}
