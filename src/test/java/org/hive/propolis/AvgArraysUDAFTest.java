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
public class AvgArraysUDAFTest {

    @Rule
    public TestName name = new TestName();

    @HiveSQL(files = {})
    private HiveShell shell;

    @Test
    public void testSimpleAverage() {
        /*
         * Insert some source data
         */
        List<Object[]> expected = shell.executeStatement(
                "select 'u' as uid, array(double(1.5), double(2.0)) as m "
                        + "union all "
                        + "select 'v' as uid, array(double(3.0)) as m"
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
                + "avg_list as 'org.hive.propolis.AvgArraysUDAF';"
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement("with t as ( "
                + "select 'u' as uid, array(1, 2) as col "
                + "union all "
                + "select 'u' as uid, array(2) as col "
                + "union all "
                + "select 'v' as uid, array(3) as col "
                + ") "
                + "select uid, avg_list(col) as m "
                + "from t group by uid"
        );

        assert(expected.size() == 2);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
        assertArrayEquals(expected.get(1), result.get(1));
    }

    @Test
    public void testNullColumn() {
        /*
         * Insert some source data
         */
        List<Object[]> expected = shell.executeStatement(
                "select 'u' as uid, array(double(1.5), double(2.0)) as m "
                        + "union all "
                        + "select 'v' as uid, array(double(3.0), NULL) as m"
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
                + "avg_list as 'org.hive.propolis.AvgArraysUDAF';"
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement("with t as ( "
                + "select 'u' as uid, array(1, 2) as col "
                + "union all "
                + "select 'u' as uid, array(2, NULL) as col "
                + "union all "
                + "select 'v' as uid, array(3, NULL) as col "
                + "union all "
                + "select 'v' as uid, array(3, NULL) as col "
                + ") "
                + "select uid, avg_list(col) as m "
                + "from t group by uid"
        );

        assert(expected.size() == 2);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
        assertArrayEquals(expected.get(1), result.get(1));
    }

    @Test
    public void testDoubleAverage() {
        /*
         * Insert some source data
         */
        List<Object[]> expected = shell.executeStatement(
                "select 'u' as uid, array(double(1.5), double(2.0)) as m "
                        + "union all "
                        + "select 'v' as uid, array(double(3.0), NULL) as m"
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
                + "avg_list as 'org.hive.propolis.AvgArraysUDAF';"
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement("with t as ( "
                + "select 'u' as uid, array(double(1.0), double(2.0)) as col "
                + "union all "
                + "select 'u' as uid, array(double(2.0), NULL) as col "
                + "union all "
                + "select 'v' as uid, array(double(3.0), NULL) as col "
                + ") "
                + "select uid, avg_list(col) as m "
                + "from t group by uid"
        );

        assert(expected.size() == 2);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
        assertArrayEquals(expected.get(1), result.get(1));
    }
}
