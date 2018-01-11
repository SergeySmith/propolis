package org.hive.propolis;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;


@Description(
        name = "Simple UDF to get rid of get parameters (queries) from URL string",
        value = "_FUNC_(url String): String -- returns URL without get parameters"
)

public class CutUrlQueryUDF extends UDF {

    public String evaluate(String url) {
        if (url == null) {
            return null;
        } else if (url.contains("?")) {
            return url.substring(0, url.lastIndexOf("?"));
        } else {
            return url;
        }
    }
}
