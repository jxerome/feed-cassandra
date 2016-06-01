import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.utils.UUIDs;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public final class Generators {
    private Generators() {
    }

    public static Supplier<String> stringSupplier(int maxLength) {
        return () -> {
            int length = ThreadLocalRandom.current().nextInt(1, maxLength);
            return new String(ThreadLocalRandom.current().ints(65, 90).limit(length).toArray(), 0, length);
        };
    }


    public static Supplier<ByteBuffer> bytesSupplier(int maxLength) {
        return () -> {
            byte[] bytes = new byte[ThreadLocalRandom.current().nextInt(maxLength)];
            ThreadLocalRandom.current().nextBytes(bytes);
            return ByteBuffer.wrap(bytes);
        };
    }

    public static Supplier<Boolean> booleanSupplier() {
        return () -> ThreadLocalRandom.current().nextBoolean();
    }

    public static Supplier<Integer> integerSupplier() {
        return () -> ThreadLocalRandom.current().nextInt();
    }

    public static Supplier<Short> shortSupplier() {
        return () -> (short) ThreadLocalRandom.current().nextInt();
    }

    public static Supplier<Byte> byteSupplier() {
        return () -> (byte) ThreadLocalRandom.current().nextInt();
    }

    public static Supplier<Long> longSupplier() {
        return () -> ThreadLocalRandom.current().nextLong();
    }

    public static Supplier<BigInteger> bigIntegerSupplier() {
        return () -> new BigInteger(128, ThreadLocalRandom.current());
    }

    public static Supplier<Double> doubleSupplier() {
        return () -> ThreadLocalRandom.current().nextDouble();
    }

    public static Supplier<Float> floatSupplier() {
        return () -> ThreadLocalRandom.current().nextFloat();
    }

    public static Supplier<BigDecimal> bigDecimalSupplier() {
        return () -> new BigDecimal(ThreadLocalRandom.current().nextDouble());
    }

    public static Supplier<UUID> timeuuidSupplier() {
        return () -> UUIDs.timeBased();
    }

    public static Supplier<UUID> uuidSupplier() {
        return () -> UUIDs.random();
    }

    public static Supplier<Date> datetimeSupplier() {
        return () -> {
            long now = System.currentTimeMillis();
            return new Date(ThreadLocalRandom.current().nextLong(now - 84000 * 10, now));
        };
    }

    public static Supplier<LocalDate> dateSupplier() {
        return () -> {
            long now = System.currentTimeMillis();
            return LocalDate.fromMillisSinceEpoch(ThreadLocalRandom.current().nextLong(now - 84000 * 10, now));
        };
    }

    public static Supplier<Long> timeSupplier() {
        return () -> ThreadLocalRandom.current().nextLong(0, TimeUnit.DAYS.toNanos(1));
    }

}
