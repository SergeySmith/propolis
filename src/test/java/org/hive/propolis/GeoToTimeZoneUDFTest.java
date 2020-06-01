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
public class GeoToTimeZoneUDFTest {

    @Rule
    public TestName name = new TestName();

    @HiveSQL(files = {})
    private HiveShell shell;

    @Test
    public void test_1() {
        List<Object[]> expected = shell.executeStatement("select 'Europe/Berlin' as tz "
                + "union all "
                + "select null as tz "
                + "union all "
                + "select 'Europe/Moscow' as tz "
        );

        shell.execute( "create temporary function "
                + "timezone as 'org.hive.propolis.GeoToTimeZoneUDF';"
        );

        List<Object[]> result = shell.executeStatement("with temp_table as ( "
                + "select double(52.52) as lat, double(13.40) as lon "
                + "union all "
                + "select null as lat, double(1.01) as lon "
                + "union all "
                + "select double(56.52) as lat, double(50.40) as lon ) "
                + "select timezone(lat, lon) as tz from temp_table");
        printResult(result);

        assert(expected.size() == 3);
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