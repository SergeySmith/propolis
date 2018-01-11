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
public class ToStringArrayUDFTest {

    @Rule
    public TestName name = new TestName();

    @HiveSQL(files = {})
    private HiveShell shell;

    @Test
    public void testBinaryWords() {
        /*
         * Insert some source data
         */
        List<Object[]> expected = shell.executeStatement("select '1' as u, array('one', 'two') as res "
                + "union all "
                + "select '2' as u, array('three') as res"
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
                + "cast_binary as 'org.hive.propolis.ToStringArrayUDF';"
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement("with t as ( "
                + "select '1' as u, cast('one' as binary) as bs "
                + "union all "
                + "select '1' as u, cast('two' as binary) as bs "
                + "union all "
                + "select '2' as u, cast('three' as binary) as bs "
                + ") "
                + "select u, cast_binary( collect_list(bs) ) as res "
                + "from t group by u"
        );

        printResult(result);

        assert(expected.size() == 2);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
        assertArrayEquals(expected.get(1), result.get(1));
    }

    @Test
    public void testBinaryLetters() {
        /*
         * Insert some source data
         */
        List<Object[]> expected = shell.executeStatement("select '1' as u, array('a', 'b') as res "
                + "union all "
                + "select '2' as u, array('c') as res"
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
                + "cast_binary as 'org.hive.propolis.ToStringArrayUDF';"
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement("with t as ( "
                + "select '1' as u, cast('a' as binary) as bs "
                + "union all "
                + "select '1' as u, cast('b' as binary) as bs "
                + "union all "
                + "select '2' as u, cast('c' as binary) as bs "
                + ") "
                + "select u, cast_binary( collect_list(bs) ) as res "
                + "from t group by u"
        );

        printResult(result);

        assert(expected.size() == 2);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
        assertArrayEquals(expected.get(1), result.get(1));
    }

    @Test
    public void testBinaryNumbers() {
        /*
         * Insert some source data
         */
        List<Object[]> expected = shell.executeStatement("select '1' as u, array('1', '2') as res "
                + "union all "
                + "select '2' as u, array('3') as res"
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
                + "cast_binary as 'org.hive.propolis.ToStringArrayUDF';"
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement("with t as ( "
                + "select '1' as u, cast('1' as binary) as bs "
                + "union all "
                + "select '1' as u, cast('2' as binary) as bs "
                + "union all "
                + "select '2' as u, cast('3' as binary) as bs "
                + ") "
                + "select u, cast_binary( collect_list(bs) ) as res "
                + "from t group by u"
        );

        printResult(result);

        assert(expected.size() == 2);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
        assertArrayEquals(expected.get(1), result.get(1));
    }

    public void printResult(List<Object[]> result) {
        System.out.println(String.format("Result from %s:",name.getMethodName()));
        for (Object[] row : result) {
            System.out.println(Arrays.asList(row));
        }
    }
}
