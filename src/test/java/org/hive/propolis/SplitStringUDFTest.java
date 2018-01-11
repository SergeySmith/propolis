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
public class SplitStringUDFTest {

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
                "select 'u' as uid, array('ml', 'abc=cba') as m "
                + "union all "
                + "select 'v' as uid, array('ml', 'abc', 'cba') as m "
                + "union all "
                + "select 'w' as uid, array('ml=abc=cba') as m "
        );

        /*
         * Execute the query
         */
        shell.execute( "set hivevar:str='ml=abc=cba'; "
                + "create temporary function "
                + "split_str as 'org.hive.propolis.SplitStringUDF';"
        );

        /*
         * Verify the result
         */
        List<Object[]> result = shell.executeStatement("select 'u' as uid, split_str(${str}, '=', 2) as m "
                + "union all "
                + "select 'v' as uid, split_str(${str}, '=', 0) as m "
                + "union all "
                + "select 'w' as uid, split_str(${str}, '=', 1) as m "
        );

        assert(expected.size() == 3);
        assertEquals(expected.size(), result.size());
        assertArrayEquals(expected.get(0), result.get(0));
        assertArrayEquals(expected.get(1), result.get(1));
        assertArrayEquals(expected.get(2), result.get(2));
    }
}
