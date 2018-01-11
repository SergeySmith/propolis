package org.hive.propolis;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Arrays;
import java.util.List;


public class SplitStringUDF extends UDF {

    public List<String> evaluate (String str, String pattern, Integer position) {
        return Arrays.asList(str.split(pattern, position));
    }
}
