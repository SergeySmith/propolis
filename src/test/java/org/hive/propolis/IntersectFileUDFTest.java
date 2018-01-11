//package org.hive.propolis;
//
//
//import com.klarna.hiverunner.HiveShell;
//import com.klarna.hiverunner.StandaloneHiveRunner;
//import com.klarna.hiverunner.annotations.HiveSQL;
//import org.junit.Rule;
//import org.junit.Test;
//import org.junit.rules.TestName;
//import org.junit.runner.RunWith;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//
//import java.lang.System;
//import java.io.ByteArrayOutputStream;
//import java.io.PrintStream;
//import java.util.Arrays;
//import java.util.List;
//
//import static org.junit.Assert.assertArrayEquals;
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertNull;
//
//
//@RunWith(StandaloneHiveRunner.class)
//public class IntersectFileUDFTest {
//
//    @Rule
//    public TestName name = new TestName();
//
//    @HiveSQL(files = {})
//    private HiveShell shell;

//    @Test
//    public void testSimpleStringArray() {
//        /*
//         * Insert some source data
//         */
//        List<Object[]> expected = shell.executeStatement("select array('one', 'two') as ar "
//                + "union all "
//                + "select array('three') as ar"
//        );
//
//        /*
//         * Execute the query
//         */
//        shell.execute( "create temporary function "
//                + "intersect_file as 'org.hive.propolis.IntersectFileUDF';"
//                + "add file /home/sergey/work/hive-custom-udfs/src/test/resources/files/intersect.tsv;"
//        );
//
//        printResult(shell.executeStatement("list files"));
//
//        /*
//         * Verify the result
//         */
//        List<Object[]> result = shell.executeStatement("with t as ( "
//                + "select array('one', 'two', 'four') as ar "
//                + "union all "
//                + "select array('three') as ar "
//                + "union all "
//                + "select array('five') as ar ) "
//                + "select intersect_file(ar, '/home/sergey/work/hive-custom-udfs/src/test/resources/files/intersect.tsv') from t"
//        );
//
//        assert(expected.size() == 2);
//        assertEquals(3, result.size());
//        assertArrayEquals(expected.get(0), result.get(0));
//        assertArrayEquals(expected.get(1), result.get(1));
//        assertNull(result.get(2));
//    }

//    @Test
//    public void testInFile() {
//        /*
//         * Insert some source data
//         */
//        List<Object[]> expected = shell.executeStatement("select true as res "
//                + "union all "
//                + "select false as res"
//        );
//
//        /*
//         * Execute the query
//         */
//        shell.execute( "add file /home/sergey/work/hive-custom-udfs/src/test/resources/files/intersect.tsv;"
//        );
//
//        printResult(shell.executeStatement("list files"));
//
//        /*
//         * Verify the result
//         */
//        List<Object[]> result = shell.executeStatement("with t as ( "
//                + "select 'one' as word "
//                + "union all "
//                + "select 'five' as word ) "
//                + "select in_file(word, '/home/sergey/work/hive-custom-udfs/src/test/resources/files/intersect.tsv') from t"
//        );
//
//        assert(expected.size() == 2);
//        assertEquals(2, result.size());
//        assertArrayEquals(expected.get(0), result.get(0));
//        assertArrayEquals(expected.get(1), result.get(1));
//    }
//
//    public void printResult(List<Object[]> result) {
//        System.out.println(String.format("Result from %s:",name.getMethodName()));
//        for (Object[] row : result) {
//            System.out.println(Arrays.asList(row));
//        }
//    }
//}
