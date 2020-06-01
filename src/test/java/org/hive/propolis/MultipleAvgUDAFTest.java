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
public class MultipleAvgUDAFTest {

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
                "select 'u' as uid, array(double(1.5), double(1.0)) as m "
                        + "union all "
                        + "select 'v' as uid, array(double(3.0), double(0.0)) as m"
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
                + "avg_all as 'org.hive.propolis.MultipleAvgUDAF';"
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement("with t as ( "
                + "select 'u' as uid, 1 as col1, 2 as col2 "
                + "union all "
                + "select 'u' as uid, 2 as col1, 0 as col2 "
                + "union all "
                + "select 'v' as uid, 3 as col1, 0 as col2 "
                + ") "
                + "select uid, avg_all(col1, col2) as m "
                + "from t group by uid"
        );

        assert(expected.size() == 2);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
        assertArrayEquals(expected.get(1), result.get(1));
    }

    @Test
    public void testAverageWithNull() {
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
                + "avg_all as 'org.hive.propolis.MultipleAvgUDAF';"
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement("with t as ( "
                + "select 'u' as uid, double(1.0) as col1, 2 as col2 "
                + "union all "
                + "select 'u' as uid, double(2.0) as col1, NULL as col2 "
                + "union all "
                + "select 'v' as uid, double(3.0) as col1, NULL as col2 "
                + ") "
                + "select uid, avg_all(col1, col2) as m "
                + "from t group by uid"
        );

        assert(expected.size() == 2);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
        assertArrayEquals(expected.get(1), result.get(1));
    }

    @Test
    public void testWithNullColumn() {
        /*
         * Insert some source data
         */
        List<Object[]> expected = shell.executeStatement(
                "select 'u' as uid, array(double(1.5), NULL) as m "
                        + "union all "
                        + "select 'v' as uid, array(double(3.0), NULL) as m"
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
                + "avg_all as 'org.hive.propolis.MultipleAvgUDAF';"
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement("with t as ( "
                + "select 'u' as uid, double(1.0) as col1, NULL as col2 "
                + "union all "
                + "select 'u' as uid, double(2.0) as col1, NULL as col2 "
                + "union all "
                + "select 'v' as uid, double(3.0) as col1, NULL as col2 "
                + ") "
                + "select uid, avg_all(col1, col2) as m "
                + "from t group by uid"
        );

        assert(expected.size() == 2);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
        assertArrayEquals(expected.get(1), result.get(1));
    }

//    @Test
//    public void testWildCardSign() {
//        /*
//         * Insert some source data
//         */
//        List<Object[]> expected = shell.executeStatement(
//                "select 'u' as uid, array(double(1.5), double(1.0)) as m "
//                        + "union all "
//                        + "select 'v' as uid, array(double(3.0), double(0.0)) as m"
//        );
//
//        /*
//         * Execute the query
//         */
//        shell.execute("create temporary function "
//                + "avg_all as 'org.hive.propolis.MultipleAvgUDAF'; "
//                + "set hive.support.quoted.identifiers=none;"
//        );
//
//        /*
//         * Verify the result
//         */
//        List<Object[]> result = shell.executeStatement("with tbl as ( "
//                + "select 'u' as uid, 1 as col1, 2 as col2 "
//                + "union all "
//                + "select 'u' as uid, 2 as col1, 0 as col2 "
//                + "union all "
//                + "select 'v' as uid, 3 as col1, 0 as col2 "
//                + ") "
//                + "select uid, avg_all(*) as m "
//                + "from tbl group by uid"
//        );
//        // `(uid)?+.+`
//
//        assert(expected.size() == 2);
//        assertEquals(expected.size(), result.size());
//        assertArrayEquals(expected.get(0), result.get(0));
//        assertArrayEquals(expected.get(1), result.get(1));
//    }
}
