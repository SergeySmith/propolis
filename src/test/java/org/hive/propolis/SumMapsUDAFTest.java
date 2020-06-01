package org.hive.propolis;


import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


@RunWith(StandaloneHiveRunner.class)
public class SumMapsUDAFTest {

    @Rule
    public TestName name = new TestName();

    @HiveSQL(files = {})
    private HiveShell shell;

    @Test
    public void testSimpleStringKeyMap() {
        /*
         * Insert some source data
         */
        List<Object[]> expected = shell.executeStatement(
                "select 'u' as uid, Map('a', 3, 'b', 2, 'c', 1) as m "
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
                + "merge_map as 'org.hive.propolis.SumMapsUDAF';"
        );

        /*
         * Verify the result
         */
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
    public void testDoubleValueMap() {
        /*
         * Insert some source data
         */
        List<Object[]> expected = shell.executeStatement(
                "select 'u' as uid, Map('a', double(3.1), 'b', double(2.2), 'c', double(1.1)) as m "
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
                + "merge_map as 'org.hive.propolis.SumMapsUDAF';"
        );

        /*
         * Verify the result
         */
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

    @Test
    public void testSimpleMapWithNulls() {
        /*
         * Insert some source data
         */
        List<Object[]> expected = shell.executeStatement(
                "select 'u' as uid, Map('a', 3, 'b', 2, 'c', 1) as m "
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
                + "merge_map as 'org.hive.propolis.SumMapsUDAF';"
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement("with t as ( "
                + "select 'u' as uid, Map('a', 1, 'b', 2, 'd', NULL) as m "
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
    public void testIntKeyMap() {
        /*
         * Insert some source data
         */
        List<Object[]> expected = shell.executeStatement(
                "select 'u' as uid, Map(1, 3, 2, 2, 3, 1) as m "
                + "union all "
                + "select 'v' as uid, Map(1,2, 2,2) as m"
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
                + "merge_map as 'org.hive.propolis.SumMapsUDAF';"
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement("with t as ( "
                + "select 'u' as uid, Map(1, 1, 2, 2) as m "
                + "union all "
                + "select 'u' as uid, Map(1, 2, 3, 1) as m "
                + "union all "
                + "select 'v' as uid, Map(1, 2) as m "
                + "union all "
                + "select 'v' as uid, Map(2, 2) as m "
                + ") "
                + "select uid, merge_map(m) as m "
                + "from t group by uid"
        );

        assert(expected.size() == 2);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
        assertArrayEquals(expected.get(1), result.get(1));
    }

    @Test
    public void testConstrainedMapStrict() {
        /*
         * Insert some source data
         */
        List<Object[]> expected = shell.executeStatement(
                "select 'u' as uid, Map('a', 3) as m "
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
                + "merge_map as 'org.hive.propolis.SumMapsUDAF';"
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement("with t as ( "
                + "select 'u' as uid, Map('a', 1, 'b', 2) as m "
                + "union all "
                + "select 'u' as uid, Map('a', 2, 'c', 1) as m "
                + ") "
                + "select uid, merge_map(m, 2, 'STRICT') as m "
                + "from t group by uid"
        );

        assert(expected.size() == 1);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
    }

    @Test
    public void testConstrainedMapNoStrict() {
        /*
         * Insert some source data
         */
        List<Object[]> expected = shell.executeStatement(
                "select 'u' as uid, Map('a', 3, 'b', 2) as m "
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
                + "merge_map as 'org.hive.propolis.SumMapsUDAF';"
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement("with t as ( "
                + "select 'u' as uid, Map('a', 1, 'b', 2) as m "
                + "union all "
                + "select 'u' as uid, Map('a', 2, 'c', 1) as m "
                + ") "
                + "select uid, merge_map(m, 2, 'NOSTRICT') as m "
                + "from t group by uid"
        );

        assert(expected.size() == 1);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
    }

    @Test
    public void testConstrainedMapNoStrictNull() {
        /*
         * Insert some source data
         */
        List<Object[]> expected = shell.executeStatement(
                "select 'u' as uid, NULL as m "
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
                + "merge_map as 'org.hive.propolis.SumMapsUDAF';"
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement("with t as ( "
                + "select 'u' as uid, Map('a', 1, 'b', 2) as m "
                + "union all "
                + "select 'u' as uid, Map('a', 2, 'c', 1) as m "
                + ") "
                + "select uid, merge_map(m, 4, 'NOSTRICT') as m "
                + "from t group by uid"
        );

        assert(expected.size() == 1);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
    }

}
