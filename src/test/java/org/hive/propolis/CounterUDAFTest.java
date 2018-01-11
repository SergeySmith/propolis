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
public class CounterUDAFTest {

    @Rule
    public TestName name = new TestName();

    @HiveSQL(files = {})
    private HiveShell shell;

    @Test
    public void testOneArgumentAggregation() {
        /*
         * Insert some source data
         */
        List<Object[]> expected = shell.executeStatement(
                "select 'u' as uid, MAP('a', 1, 'b', 2) as m "
                + "union all "
                + "select 'v' as uid, MAP('c', 2) as m"
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
                + "counter as 'org.hive.propolis.CounterUDAF';"
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement("with t as ( "
                + "select 'u' as uid, 'b' as col "
                + "union all "
                + "select 'u' as uid, 'a' as col "
                + "union all "
                + "select 'u' as uid, 'b' as col "
                + "union all "
                + "select 'v' as uid, 'c' as col "
                + "union all "
                + "select 'v' as uid, 'c' as col "
                + ") "
                + "select uid, counter(col) as m "
                + "from t group by uid"
        );

        assert(expected.size() == 2);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
        assertArrayEquals(expected.get(1), result.get(1));
    }

    @Test
    public void testTwoArgumentsAggregation() {
        /*
         * Insert some source data
         */
        List<Object[]> expected = shell.executeStatement(
                "select 'u' as uid, MAP('a', 2, 'b', 2) as m "
                        + "union all "
                        + "select 'v' as uid, MAP('a', 1, 'b', 1, 'c', 2) as m "
                        + "union all "
                        + "select 'w' as uid, MAP('a', 3, 'b', 2, 'c', 1) as m "
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
                + "counter as 'org.hive.propolis.CounterUDAF';"
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement("with t as ( "
                + "select 'u' as uid, 'b' as col, 'b' as col1 "
                + "union all "
                + "select 'u' as uid, 'a' as col, 'a' as col1 "
                + "union all "
                + "select 'v' as uid, 'c' as col, 'b' as col1 "
                + "union all "
                + "select 'w' as uid, 'a' as col, 'b' as col1 "
                + "union all "
                + "select 'v' as uid, 'a' as col, 'c' as col1 "
                + "union all "
                + "select 'w' as uid, 'a' as col, 'b' as col1 "
                + "union all "
                + "select 'w' as uid, 'a' as col, 'c' as col1 "
                + ") "
                + "select uid, counter(col, col1) as m "
                + "from t group by uid order by uid"
        );

        assert(expected.size() == 3);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
        assertArrayEquals(expected.get(1), result.get(1));
        assertArrayEquals(expected.get(2), result.get(2));
    }

    @Test
    public void testNullAggregation() {
        /*
         * Insert some source data
         */
        List<Object[]> expected = shell.executeStatement(
                "select 'u' as uid, MAP('a', 1, 'b', 2) as m "
                        + "union all "
                        + "select 'v' as uid, MAP('c', 2) as m"
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
                + "counter as 'org.hive.propolis.CounterUDAF';"
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement("with t as ( "
                + "select 'u' as uid, 'b' as col "
                + "union all "
                + "select 'u' as uid, 'a' as col "
                + "union all "
                + "select 'u' as uid, 'b' as col "
                + "union all "
                + "select 'v' as uid, 'c' as col "
                + "union all "
                + "select 'v' as uid, NULL as col "
                + "union all "
                + "select 'v' as uid, 'c' as col "
                + ") "
                + "select uid, counter(col) as m "
                + "from t group by uid"
        );

        assert(expected.size() == 2);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
        assertArrayEquals(expected.get(1), result.get(1));
    }
}
