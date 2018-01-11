package org.hive.propolis;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


@RunWith(StandaloneHiveRunner.class)
public class ArrayStructDistinctUDFTest {

    @Rule
    public TestName name = new TestName();

    @HiveSQL(files = {})
    private HiveShell shell;

    @Test
    public void testSimpleMap() {
        /*
         * Insert some source data
         */
        List<Object[]> expected = shell.executeStatement(
                "select 'u' as uid, array(named_struct('lat', 2.0, 'lon', 1.0), named_struct('lat', 2.0, 'lon', 2.0) ) as m "
                + "union all "
                + "select 'v' as uid, array(named_struct('lat', 2.0, 'lon', 1.0) ) as m "
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
            + "array_distinct as 'org.hive.propolis.ArrayStructDistinctUDF';"
            + "create temporary table data stored as ORC as "
            + "select 'u' as uid, array(named_struct('lat', 2.0, 'lon', 1.0), named_struct('lat', 2.0, 'lon', 1.0), named_struct('lat', 2.0, 'lon', 2.0)) as ar "
            + "union all "
            + "select 'v' as uid, array(named_struct('lat', 2.0, 'lon', 1.0), named_struct('lat', 2.0, 'lon', 1.0), named_struct('lat', 2.0, 'lon', 1.0)) as ar "
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement(
                "select uid, array_distinct( ar, array('lat', 'lon') ) as m "
                + "from data "
        );

        printResult(result);
        printResult(expected);

        assert(expected.size() == 2);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
        assertArrayEquals(expected.get(1), result.get(1));
    }

    @Test
    public void testAnotherSimpleMap() {
        /*
         * Insert some source data
         */
        List<Object[]> expected = shell.executeStatement(
                "select 'u' as uid, array(named_struct('a', 2.0, 'b', 1.0, 'c', 0.0), named_struct('a', 2.0, 'b', 2.0, 'c', 1.0) ) as m "
                        + "union all "
                        + "select 'v' as uid, array(named_struct('a', 2.0, 'b', 1.0, 'c', 0.0) ) as m "
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
                + "array_distinct as 'org.hive.propolis.ArrayStructDistinctUDF';"
                + "create temporary table data stored as ORC as "
                + "select 'u' as uid, array(named_struct('a', 2.0, 'b', 1.0, 'c', 0.0), named_struct('a', 2.0, 'b', 1.0, 'c', 0.0), named_struct('a', 2.0, 'b', 2.0, 'c', 1.0)) as ar "
                + "union all "
                + "select 'v' as uid, array(named_struct('a', 2.0, 'b', 1.0, 'c', 0.0), named_struct('a', 2.0, 'b', 1.0, 'c', 0.0), named_struct('a', 2.0, 'b', 1.0, 'c', 0.0)) as ar "
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement(
                "select uid, array_distinct( ar, array('a', 'b', 'c') ) as m "
                        + "from data "
        );

        printResult(result);
        printResult(expected);

        assert(expected.size() == 2);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
        assertArrayEquals(expected.get(1), result.get(1));
    }

//    @Test
//    public void testMapWithNulls() {
//        /*
//         * Insert some source data
//         */
//        List<Object[]> expected = shell.executeStatement(
//                "select 'u' as uid, array(named_struct('a', 2.0, 'b', NULL, 'c', 0.0), named_struct('a', 2.0, 'b', NULL, 'c', 1.0) ) as m "
//                        + "union all "
//                        + "select 'v' as uid, array(named_struct('a', 2.0, 'b', NULL, 'c', 0.0) ) as m "
//        );
//
//        /*
//         * Execute the query
//         */
//        shell.execute( "create temporary function "
//                + "array_distinct as 'org.hive.propolis.ArrayStructDistinctUDF';"
//                + "create temporary table data stored as ORC as "
//                + "select 'u' as uid, array(named_struct('a', 2.0, 'b', NULL, 'c', 0.0), named_struct('a', 2.0, 'b', NULL, 'c', 0.0), named_struct('a', 2.0, 'b', NULL, 'c', 1.0)) as ar "
//                + "union all "
//                + "select 'v' as uid, array(named_struct('a', 2.0, 'b', NULL, 'c', 0.0), named_struct('a', 2.0, 'b', NULL, 'c', 0.0), named_struct('a', 2.0, 'b', NULL, 'c', 0.0)) as ar "
//        );
//
//        /*
//         * Verify the result
//         */
//        List<Object[]> result = shell.executeStatement(
//                "select uid, array_distinct( ar, array('a', 'b', 'c') ) as m "
//                        + "from data "
//        );
//
//        printResult(result);
//        printResult(expected);
//
//        assert(expected.size() == 2);
//        assertEquals(expected.size(), result.size());
//        assertArrayEquals(expected.get(0), result.get(0));
//        assertArrayEquals(expected.get(1), result.get(1));
//    }

    public void printResult(List<Object[]> result) {
        System.out.println(String.format("Result from %s:",name.getMethodName()));
        for (Object[] row : result) {
            System.out.println(Arrays.asList(row));
        }
    }
}
