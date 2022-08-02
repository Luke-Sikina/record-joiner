import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class RecordJoinerTest {

    RecordJoiner subject = new RecordJoiner();

    @Test
    void shouldJoin() {
        List<RecordA> as = List.of(new RecordA(1, "foo"));
        List<RecordB> bs = List.of(new RecordB(1, "bar"));

        List<RecordAB> actual = subject.join(as, RecordA::key, bs, RecordB::key, RecordAB.class);
        List<RecordAB> expected = List.of(new RecordAB(1, "foo", "bar"));

        Assertions.assertEquals(expected, actual);
    }
}