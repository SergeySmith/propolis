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
public class CutUrlQueryUDFTest {

    @Rule
    public TestName name = new TestName();

    @HiveSQL(files = {})
    private HiveShell shell;

    @Test
    public void testUrlWithQuery() {
        /*
         * Insert some source data
         */
        List<Object[]> expected = shell.executeStatement(
                "select 'http://www.xyz/path1/path2/path3' as url "
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
                + "cut_query as 'org.hive.propolis.CutUrlQueryUDF';"
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement(
                "select cut_query('http://www.xyz/path1/path2/path3?param1=value1&param2=value2') as url "
        );

        assert(expected.size() == 1);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
    }

    @Test
    public void testUrlWithoutQuery() {
        /*
         * Insert some source data
         */
        List<Object[]> expected = shell.executeStatement(
                "select 'http://www.xyz/path1/path2/path3' as url "
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
                + "cut_query as 'org.hive.propolis.CutUrlQueryUDF';"
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement(
                "select cut_query('http://www.xyz/path1/path2/path3') as url "
        );

        assert(expected.size() == 1);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
    }

    @Test
    public void testNull() {
        /*
         * Insert some source data
         */
        List<Object[]> expected = shell.executeStatement(
                "select NULL as url "
        );

        /*
         * Execute the query
         */
        shell.execute( "create temporary function "
                + "cut_query as 'org.hive.propolis.CutUrlQueryUDF';"
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement(
                "select cut_query(NULL) as url "
        );

        assert(expected.size() == 1);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
    }
}
