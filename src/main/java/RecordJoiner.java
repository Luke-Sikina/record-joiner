import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Since we have two different databases running two entirely different flavors of SQL
 * we need a generic way to join rows from those two databases.
 * RecordJoiner accepts left and right rows, a method handle from left and right for the
 * key to join on, and the class of the record that should result from joining left and
 * right.
 */
public class RecordJoiner {

    /**
     * Given two records RA and RB, make a function that gets a value from one of them.
     * Which one is up to the implementer.
     * @param <RA> record A
     * @param <RB> record B
     */
    @FunctionalInterface
    public interface DualRecordAccessor<RA extends Record, RB extends Record> {
        Object get(Pair<RA,RB> pair);
    }

    /**
     * A pair of things. For streams
     */
    private record Pair<F, S>(F first, S second){}


    /**
     * Performs an inner join on the rows of L and R, resulting in rows of X, where X is
     * a record that contains a key field that it shares with at least L, and a set of other fields.
     * Other fields that share a name with a field in L will be populated with the corresponding L value.
     * Other fields that share a name with a field in R will be populated with the corresponding R value.
     * Other fields will be assigned their zero value (null, 0, 0f, etc.)
     *
     * @param left a list of rows to be joined
     * @param leftKey the function to get a key value from a left row
     * @param right a list of rows to be joined
     * @param rightKey the function to get a key value from a right row
     * @param joinedRecordType the class of the resulting records when joining left and right
     * @return rows of type joinedRecordType
     * @param <V> the type of the common key
     */
    public <L extends Record, R extends Record, X extends Record, V> List<X> innerJoin(
        List<L> left, RecordFieldHandle<L, V> leftKey,
        List<R> right, RecordFieldHandle<R, V> rightKey,
        Class<X> joinedRecordType
    ) {
        if (left.isEmpty() || right.isEmpty()) {
            return List.of();
        }
        // create method mappings
        Map<String, Method> lAccessors = getAccessors(left.get(0).getClass());
        Map<String, Method> rAccessors = getAccessors(right.get(0).getClass());

        // using the two method mappings, for each field in X's constructor,
        // pick a method from aAccessors or bAccessors, or use a stub
        Map<String, DualRecordAccessor<L, R>> joinedAccessors = joinAccessors(joinedRecordType, lAccessors, rAccessors);
        Constructor<X> joiningConstructor = getCanonicalConstructor(joinedRecordType);
        List<DualRecordAccessor<L, R>> orderedAccessors = Arrays.stream(joiningConstructor.getParameters())
            .map(param -> joinedAccessors.get(param.getName()))
            .toList();

        // group the rows of a and b by their keys
        Map<V, List<L>> groupedL = left.stream().collect(Collectors.groupingBy(leftKey::apply));
        Map<V, List<R>> groupedR = right.stream().collect(Collectors.groupingBy(rightKey::apply));

        // for each common key, make a new row X for each permutation of a and b that share that key
        return groupedL.entrySet().stream()
            .flatMap(e -> permute(e.getValue(), groupedR.getOrDefault(e.getKey(), List.of())))
            .flatMap(pair -> {
                Object[] constructorArgs = orderedAccessors.stream()
                    .map(method -> method.get(pair))
                    .toArray();
                try {
                    return Stream.of(joiningConstructor.newInstance(constructorArgs));
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                    return Stream.of();
                }
            })
            .toList();
    }

    private <L extends Record, R extends Record> Stream<Pair<L, R>> permute(List<L> l, List<R> r) {
        return l.stream()
            .flatMap(aa -> r.stream().map(bb -> new Pair<>(aa, bb)));
    }

    private <A extends Record, B extends Record, X extends Record> Map<String, DualRecordAccessor<A, B>> joinAccessors(
        Class<X> joined, Map<String, Method> aAccessors, Map<String, Method> bAccessors
    ) {
        return getAccessors(joined).entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> {
                final boolean useFirst = aAccessors.containsKey(e.getKey());
                final Method toExecute = useFirst ? aAccessors.get(e.getKey()) : bAccessors.get(e.getKey());
                if (toExecute == null) {
                    return (pair) -> getPrimitiveSafeValue(e.getValue().getReturnType());
                } else {
                    return (pair) -> {
                        try {
                            return toExecute.invoke(useFirst ? pair.first : pair.second);
                        } catch (IllegalAccessException | InvocationTargetException ex) {
                            throw new RuntimeException(ex);
                        }
                    };
                }
            }
        ));
    }

    private Object getPrimitiveSafeValue(Class<?> returnType) {
        // ensures that unboxing primitive doesn't cause a NPE
        if (!returnType.isPrimitive()) {
            return null;
        } else if (returnType.equals(int.class)) {
            return 0;
        } else if (returnType.equals(short.class)){
            return (short) 0;
        } else if (returnType.equals(byte.class)) {
            return (byte) 0;
        } else if (returnType.equals(long.class)) {
            return 0L;
        } else if (returnType.equals(float.class)) {
            return 0.0f;
        } else if (returnType.equals(double.class)) {
            return 0.0d;
        } else if (returnType.equals(char.class)) {
            return (char) 0;
        } else {
            return false;
        }
    }

    private <R extends Record> Map<String, Method> getAccessors(Class<R> recordClass) {
        return Arrays.stream(recordClass.getRecordComponents())
            .collect(Collectors.toMap(
                RecordComponent::getName,
                RecordComponent::getAccessor
            ));
    }

    private <R extends Record> Constructor<R> getCanonicalConstructor(Class<R> clazz) {
        // The canonical constructor will have all the record's components. getRecordComponents
        // returns all those record components in the order that they appear in the constructor
        Class<?>[] componentTypes = Arrays.stream(clazz.getRecordComponents())
            .map(RecordComponent::getType)
            .toArray(Class<?>[]::new);
        try {
            return clazz.getDeclaredConstructor(componentTypes);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}
