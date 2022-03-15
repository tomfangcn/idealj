package bootstrap.test.function;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MyTimeWindow extends TableFunction<String> {

    private SimpleDateFormat format;

    @Override
    public void open(FunctionContext context) throws Exception {
        format = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    }


    public void eval(String str, Integer n) throws ParseException {

        for (int i = 0; i < n; i++) {
            collect(format.format(new Date(format.parse(str).getTime() + (long) i * 1000 * 60)));
        }

    }

}
