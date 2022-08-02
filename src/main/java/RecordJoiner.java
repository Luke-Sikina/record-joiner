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
    public <A extends Record, B extends Record, X extends Record, V> List<X> join(
        List<A> a, RecordFieldHandle<A, V> fieldA,
        List<B> b, RecordFieldHandle<B, V> fieldB,
        Class<X> joined
    ) {
        if (a.isEmpty() || b.isEmpty()) {
            return List.of();
        }
        // create method mappings
        Map<String, Method> aAccessors = getAccessors(a.get(0).getClass());
        Map<String, Method> bAccessors = getAccessors(b.get(0).getClass());
        Map<String, DualRecordAccessor<A, B>> joinedAccessors = joinAccessors(joined, aAccessors, bAccessors);
        Constructor<X> joiningConstructor = getCanonicalConstructor(joined);
        List<DualRecordAccessor<A, B>> orderedAccessors = Arrays.stream(joiningConstructor.getParameters())
            .map(param -> joinedAccessors.get(param.getName()))
            .toList();

        Map<V, List<A>> groupedA = a.stream().collect(Collectors.groupingBy(fieldA::apply));
        Map<V, List<B>> groupedB = b.stream().collect(Collectors.groupingBy(fieldB::apply));

        return groupedA.entrySet().stream()
            .flatMap(e -> permute(e.getValue(), groupedB.getOrDefault(e.getKey(), List.of())))
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

    @FunctionalInterface
    private interface DualRecordAccessor<RA extends Record, RB extends Record> {
        Object get(Pair<RA,RB> pair);
    }

    private record Pair<F, S>(F first, S second){};

    private <A extends Record, B extends Record> Stream<Pair<A, B>> permute(List<A> a, List<B> b) {
        return a.stream()
            .flatMap(aa -> b.stream().map(bb -> new Pair<>(aa, bb)));
    }

    private <A extends Record, B extends Record, X extends Record> Map<String, DualRecordAccessor<A, B>> joinAccessors(
        Class<X> joined, Map<String, Method> aAccessors, Map<String, Method> bAccessors
    ) {
        return getAccessors(joined).entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> {
                final Method methodA = aAccessors.get(e.getKey());
                final Method methodB = bAccessors.get(e.getKey());
                if (methodA != null) {
                    return (pair) -> {
                        try {
                            return methodA.invoke(pair.first);
                        } catch (IllegalAccessException | InvocationTargetException ex) {
                            throw new RuntimeException(ex);
                        }
                    };
                } else if (methodB != null) {
                    return (pair) -> {
                        try {
                            return methodB.invoke(pair.second);
                        } catch (IllegalAccessException | InvocationTargetException ex) {
                            throw new RuntimeException(ex);
                        }
                    };
                } else {
                    // TODO: this will explode for primitives
                    return (pair) -> null;
                }
            }
        ));
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
