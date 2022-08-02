import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class RecordJoinerTest {
    public record RecordA(int key, String valA) {}

    public record RecordAWithExtraField(int key, String valA, long notInAB) {}
    public record RecordB(int key, String valB) {}
    public record RecordAB(int key, String valA, String valB) {}

    public record RecordABAndNullField(int key, String valA, String valB, float notInAOrB) {}

    RecordJoiner subject = new RecordJoiner();

    @Test
    void shouldJoin() {
        List<RecordA> as = List.of(new RecordA(1, "foo"));
        List<RecordB> bs = List.of(new RecordB(1, "bar"));

        List<RecordAB> actual = subject.innerJoin(as, RecordA::key, bs, RecordB::key, RecordAB.class);
        List<RecordAB> expected = List.of(new RecordAB(1, "foo", "bar"));

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void shouldJoinAndIgnoreExtraFieldInResult() {
        List<RecordA> as = List.of(new RecordA(1, "foo"));
        List<RecordB> bs = List.of(new RecordB(1, "bar"));

        List<RecordABAndNullField> actual = subject.innerJoin(as, RecordA::key, bs, RecordB::key, RecordABAndNullField.class);
        List<RecordABAndNullField> expected = List.of(new RecordABAndNullField(1, "foo", "bar", 0f));

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void shouldJoinAndIgnoreExtraFieldInA() {
        List<RecordAWithExtraField> as = List.of(new RecordAWithExtraField(1, "foo", 0L));
        List<RecordB> bs = List.of(new RecordB(1, "bar"));

        List<RecordAB> actual = subject.innerJoin(as, RecordAWithExtraField::key, bs, RecordB::key, RecordAB.class);
        List<RecordAB> expected = List.of(new RecordAB(1, "foo", "bar"));

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void shouldJoinOneToMany() {
        List<RecordA> as = List.of(new RecordA(1, "foo"));
        List<RecordB> bs = List.of(new RecordB(1, "bar"), new RecordB(1, "baz"));

        List<RecordAB> actual = subject.innerJoin(as, RecordA::key, bs, RecordB::key, RecordAB.class);
        List<RecordAB> expected = List.of(new RecordAB(1, "foo", "bar"), new RecordAB(1, "foo", "baz"));

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void shouldJoinManyToMany() {
        List<RecordA> as = List.of(new RecordA(1, "foo"), new RecordA(1, "fah"));
        List<RecordB> bs = List.of(new RecordB(1, "bar"), new RecordB(1, "baz"));

        List<RecordAB> actual = subject.innerJoin(as, RecordA::key, bs, RecordB::key, RecordAB.class);
        List<RecordAB> expected = List.of(
            new RecordAB(1, "foo", "bar"), new RecordAB(1, "foo", "baz"),
            new RecordAB(1, "fah", "bar"), new RecordAB(1, "fah", "baz")
        );

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void shouldJoinManyToOne() {
        List<RecordA> as = List.of(new RecordA(1, "foo"), new RecordA(1, "fah"));
        List<RecordB> bs = List.of(new RecordB(1, "bar"));

        List<RecordAB> actual = subject.innerJoin(as, RecordA::key, bs, RecordB::key, RecordAB.class);
        List<RecordAB> expected = List.of(new RecordAB(1, "foo", "bar"), new RecordAB(1, "fah", "bar"));

        Assertions.assertEquals(expected, actual);
    }

    @Test
    void shouldNotJoinIfEmpty() {
        List<RecordA> as = List.of();
        List<RecordB> bs = List.of(new RecordB(1, "bar"));

        List<RecordAB> actual = subject.innerJoin(as, RecordA::key, bs, RecordB::key, RecordAB.class);
        List<RecordAB> expected = List.of();

        Assertions.assertEquals(expected, actual);
    }
}