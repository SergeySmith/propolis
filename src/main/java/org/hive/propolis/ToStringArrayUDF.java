package org.hive.propolis;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.io.*;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.io.*;

import java.util.ArrayList;
import java.util.List;

/**
 * forked from Hive's GitHub
 * TODO:
 * generalize to GenericUDF
 */

public class ToStringArrayUDF extends UDF {
    private final Text t = new Text();
    private final ByteStream.Output out = new ByteStream.Output();

    public ToStringArrayUDF() {
    }

    public Text evaluate(NullWritable i) {
        return null;
    }

    private final byte[] trueBytes = {'T', 'R', 'U', 'E'};
    private final byte[] falseBytes = {'F', 'A', 'L', 'S', 'E'};

    public Text evaluate(BooleanWritable i) {
        if (i == null) {
            return null;
        } else {
            t.clear();
            t.set(i.get() ? trueBytes : falseBytes);
            return t;
        }
    }

    public Text evaluate(ShortWritable i) {
        if (i == null) {
            return null;
        } else {
            out.reset();
            LazyInteger.writeUTF8NoException(out, i.get());
            t.set(out.getData(), 0, out.getLength());
            return t;
        }
    }

    public Text evaluate(IntWritable i) {
        if (i == null) {
            return null;
        } else {
            out.reset();
            LazyInteger.writeUTF8NoException(out, i.get());
            t.set(out.getData(), 0, out.getLength());
            return t;
        }
    }

    public Text evaluate(LongWritable i) {
        if (i == null) {
            return null;
        } else {
            out.reset();
            LazyLong.writeUTF8NoException(out, i.get());
            t.set(out.getData(), 0, out.getLength());
            return t;
        }
    }

    public Text evaluate(FloatWritable i) {
        if (i == null) {
            return null;
        } else {
            t.set(i.toString());
            return t;
        }
    }

    public Text evaluate(DoubleWritable i) {
        if (i == null) {
            return null;
        } else {
            t.set(i.toString());
            return t;
        }
    }

    public Text evaluate(Text i) {
        if (i == null) {
            return null;
        }
        i.set(i.toString());
        return i;
    }

    public Text evaluate(DateWritable d) {
        if (d == null) {
            return null;
        } else {
            t.set(d.toString());
            return t;
        }
    }

    public Text evaluate(TimestampWritable i) {
        if (i == null) {
            return null;
        } else {
            t.set(i.toString());
            return t;
        }
    }

    public Text evaluate(HiveDecimalWritable i) {
        if (i == null) {
            return null;
        } else {
            t.set(i.toString());
            return t;
        }
    }

    public List<Text> evaluate (List<BytesWritable> bwl) {
        if (null == bwl) {
            return null;
        }
        List<Text> result = new ArrayList<Text>();
        for (BytesWritable bw: bwl) {
            Text tx = new Text();
            tx.set(bw.getBytes(), 0, bw.getLength());
            result.add(tx);
        }
        return result;
    }
}
