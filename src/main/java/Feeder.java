import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class Feeder implements Runnable {
    private Session session;
    private PreparedStatement insert;
    private List<Supplier<? extends Object>> generators = new ArrayList<>();
    private int iterations;

    private Feeder() {
    }

    @Override
    public void run() {
        for (int i = 0; i < iterations; ++i) {
            session.execute(insert.bind(generators.stream().map(Supplier::get).toArray()));
        }
    }

    public static Feeder build(Consumer<Builder> init) {
        Builder builder = new Builder();
        init.accept(builder);
        return builder.build();
    }

    public static class Builder {
        private Feeder feeder = new Feeder();

        private Builder() {
        }

        public Builder withSession(Session session) {
            feeder.session = session;
            return this;
        }

        public Builder withInsert(PreparedStatement insert) {
            feeder.insert = insert;
            return this;
        }

        public Builder withGenerators(List<Supplier<? extends Object>> generator) {
            feeder.generators.addAll(generator);
            return this;
        }

        public Builder withIterations(int iterations) {
            feeder.iterations = iterations;
            return this;
        }

        private Feeder build() {
            return feeder;
        }
    }

}
