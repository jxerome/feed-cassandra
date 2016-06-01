import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public final class FeedCassandra implements Runnable {
    @Parameter(names = {"-h", "--host"}, required = true)
    private String host;

    @Parameter(names = {"-k", "--keyspace"})
    private String keyspace;

    @Parameter(names = {"-n", "--iterations"})
    private int iterations = 100;

    @Parameter(required = true)
    private List<String> tables;

    @Override
    public void run() {
        try (Cluster cluster = startCluster();
             Session session = cluster.connect()) {
            for (String tableFullName : tables) {
                try {
                    Table table = extractTable(tableFullName);
                    List<ColumnMetadata> columns = extractColumns(table, cluster);

                    Feeder feeder = Feeder.build(b -> b
                        .withSession(session)
                        .withIterations(iterations)
                        .withInsert(prepareStatement(table, columns, session))
                        .withGenerators(prepareGenerators(columns))
                    );

                    feeder.run();

                } catch (FeedException e) {
                    System.err.println(e.getMessage());
                }
            }
        }
    }

    private Cluster startCluster() {
        Cluster.Builder builder = Cluster.builder()
            .addContactPoint(host);

        return builder.build();
    }

    private Table extractTable(String tableFullName) throws FeedException {
        if (tableFullName.contains(".")) {
            String[] parts = tableFullName.split("\\.", 2);
            return new Table(parts[0], parts[1]);
        }

        if (keyspace != null) {
            return new Table(keyspace, tableFullName);
        }

        throw new FeedException(String.format("Table %s ignored : no keyspace provided", tableFullName));
    }

    private List<ColumnMetadata> extractColumns(Table table, Cluster cluster) throws FeedException {
        KeyspaceMetadata keyspaceMd = cluster.getMetadata().getKeyspace(table.getKeyspace());
        if (keyspaceMd == null) {
            throw new FeedException(String.format("Table %s ignored : keyspace %s doesn't exist", table, table.getKeyspace()));
        }
        TableMetadata tableMd = keyspaceMd.getTable(table.getTable());
        if (tableMd == null) {
            throw new FeedException(String.format("Table %s ignored : the table doesn't exist", table));
        }
        return tableMd.getColumns();
    }

    private PreparedStatement prepareStatement(Table table, List<ColumnMetadata> cols, Session session) {
        Insert insert = QueryBuilder.insertInto(table.getKeyspace(), table.getTable());
        cols.forEach(c -> insert.value(c.getName(), QueryBuilder.bindMarker()));
        return session.prepare(insert);
    }

    private List<Supplier<?>> prepareGenerators(List<ColumnMetadata> columns) {
        return columns.stream().map(this::prepareGenerator).collect(Collectors.toList());
    }


    private Supplier<? extends Object> prepareGenerator(ColumnMetadata column) throws FeedException {
        switch (column.getType().getName()) {
            case ASCII:
            case VARCHAR:
            case TEXT:
                return Generators.stringSupplier(100);
            case BLOB:
                return Generators.bytesSupplier(100);
            case BOOLEAN:
                return Generators.booleanSupplier();
            case VARINT:
                return Generators.bigIntegerSupplier();
            case BIGINT:
                return Generators.longSupplier();
            case DECIMAL:
                return Generators.bigDecimalSupplier();
            case DOUBLE:
                return Generators.doubleSupplier();
            case FLOAT:
                return Generators.floatSupplier();
            case INT:
                return Generators.integerSupplier();
            case SMALLINT:
                return Generators.shortSupplier();
            case TINYINT:
                return Generators.byteSupplier();
            case UUID:
                return Generators.uuidSupplier();
            case TIMEUUID:
                return Generators.timeuuidSupplier();
            case TIMESTAMP:
                return Generators.datetimeSupplier();
            case DATE:
                return Generators.dateSupplier();
            case TIME:
                return Generators.timeSupplier();

            case LIST:
            case SET:
            case MAP:
            case INET:
            case CUSTOM:
            case UDT:
            case TUPLE:
            case COUNTER:
            default:
                throw new FeedException(String.format("%s datatype of column %s is not supported", column.getType(), column.getName()));
        }
    }

    public static void main(String[] args) {
        try {
            FeedCassandra feeder = new FeedCassandra();
            new JCommander(feeder).parse(args);
            feeder.run();
        } catch (ParameterException e) {
            System.err.println("Invalid parameters : " + e.getMessage());
        }
    }

}
