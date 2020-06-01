package org.hive.propolis;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


@RunWith(StandaloneHiveRunner.class)
public class MergeMapsUDAFTest {

    @Rule
    public TestName name = new TestName();

    @HiveSQL(files = {})
    private HiveShell shell;

    @Test
    public void testSimpleStringKeyMap() {
        List<Object[]> expected = shell.executeStatement(
                "select 'u' as uid, Map('a', 3, 'b', 2, 'c', 1) as m "
        );

        shell.execute( "create temporary function "
                + "merge_map as 'org.hive.propolis.MergeMapsUDAF';"
        );

        List<Object[]> result = shell.executeStatement("with t as ( "
                + "select 'u' as uid, Map('a', 1, 'b', 2) as m "
                + "union all "
                + "select 'u' as uid, Map('a', 2, 'c', 1) as m "
                + ") "
                + "select uid, merge_map(m) as m "
                + "from t group by uid"
        );

        assert(expected.size() == 1);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
    }

    @Test
    public void testTinyIntMap() {
        List<Object[]> expected = shell.executeStatement(
                "select 'u' as uid, Map('a', 3, 'b', 2, 'c', 1) as m "
        );

        shell.execute( "create temporary function "
                + "merge_map as 'org.hive.propolis.MergeMapsUDAF';"
        );

        List<Object[]> result = shell.executeStatement("with t as ( "
                + "select 'u' as uid, Map('a', cast(1 as tinyint), 'b', cast(2 as tinyint)) as m "
                + "union all "
                + "select 'u' as uid, Map('a', cast(2 as tinyint), 'c', cast(1 as tinyint)) as m "
                + ") "
                + "select uid, merge_map(m) as m "
                + "from t group by uid"
        );

        assert(expected.size() == 1);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
    }

    @Test
    public void testSmallIntMap() {
        List<Object[]> expected = shell.executeStatement(
                "select 'u' as uid, Map('a', 3, 'b', 2, 'c', 1) as m "
        );

        shell.execute( "create temporary function "
                + "merge_map as 'org.hive.propolis.MergeMapsUDAF';"
        );

        List<Object[]> result = shell.executeStatement("with t as ( "
                + "select 'u' as uid, Map('a', cast(1 as smallint), 'b', cast(2 as smallint)) as m "
                + "union all "
                + "select 'u' as uid, Map('a', cast(2 as smallint), 'c', cast(1 as smallint)) as m "
                + ") "
                + "select uid, merge_map(m) as m "
                + "from t group by uid"
        );

        assert(expected.size() == 1);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
    }

    @Test
    public void testBigIntMap() {
        List<Object[]> expected = shell.executeStatement(
                "select 'u' as uid, Map('a', cast(3 as bigint), 'b', cast(2 as bigint), 'c', cast(1 as bigint)) as m "
        );

        shell.execute( "create temporary function "
                + "merge_map as 'org.hive.propolis.MergeMapsUDAF';"
        );

        List<Object[]> result = shell.executeStatement("with t as ( "
                + "select 'u' as uid, Map('a', cast(1 as bigint), 'b', cast(2 as bigint)) as m "
                + "union all "
                + "select 'u' as uid, Map('a', cast(2 as bigint), 'c', cast(1 as bigint)) as m "
                + ") "
                + "select uid, merge_map(m) as m "
                + "from t group by uid"
        );

        assert(expected.size() == 1);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
    }

    @Test
    public void testDoubleValueMap() {
        List<Object[]> expected = shell.executeStatement(
                "select 'u' as uid, Map('a', double(3.1), 'b', double(2.2), 'c', double(1.1)) as m "
        );

        shell.execute( "create temporary function "
                + "merge_map as 'org.hive.propolis.MergeMapsUDAF';"
        );

        List<Object[]> result = shell.executeStatement("with t as ( "
                + "select 'u' as uid, Map('a', double(1.1), 'b', double(2.2)) as m "
                + "union all "
                + "select 'u' as uid, Map('a', double(2.0), 'c', double(1.1)) as m "
                + ") "
                + "select uid, merge_map(m) as m "
                + "from t group by uid"
        );

        assert(expected.size() == 1);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
    }
}
