import java.io.Serializable;

@FunctionalInterface
public interface RecordFieldHandle<R extends Record, V> extends Serializable {
    /**
     * @return the value in the record that corresponds to this field
     */
    V apply(R record);
}