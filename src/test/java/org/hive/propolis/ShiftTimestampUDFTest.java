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
public class ShiftTimestampUDFTest {

    @Rule
    public TestName name = new TestName();

    @HiveSQL(files = {})
    private HiveShell shell;

    @Test
    public void test_1() {
        List<Object[]> expected = shell.executeStatement("select cast('1970-01-17 10:03:22.875' as timestamp) as ts "
                + "union all "
                + "select null as ts "
        );

        shell.execute( "create temporary function "
                + "shift_ts as 'org.hive.propolis.ShiftTimestampUDF';"
        );

        List<Object[]> result = shell.executeStatement("with temp_table as ( "
                + "select 1389802875 as ts, 'UTC' as to_zone, 'America/Montreal' as from_zone "
                + "union all "
                + "select 1234556789 as ts, NULL as to_zone, 'America/Montreal' as from_zone ) "
                + "select shift_ts(ts, to_zone, from_zone) as ts from temp_table");
        printResult(result);

        assert(expected.size() == 2);
        assertEquals(expected.size(), result.size());
        for (int i = 0; i < result.size(); ++i) {
            assertArrayEquals(expected.get(i), result.get(i));
        }
    }

    public void printResult(List<Object[]> result) {
        System.out.println(String.format("Result from %s:",name.getMethodName()));
        for (Object[] row : result) {
            System.out.println(Arrays.asList(row));
        }
    }
}
