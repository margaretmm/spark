package flink.datastream.sources;

import flink.datastream.datatypes.DataFriday;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class FridaySource extends BaseSource<DataFriday> {

    public FridaySource(String dataFilePath) {
        super(dataFilePath, 1);
    }

    public FridaySource(String dataFilePath, int servingSpeedFactor) {
        super(dataFilePath,servingSpeedFactor);
    }

    public long getEventTime(DataFriday diag) {
        return diag.getEventTime();
    }

    @Override
    public void run(SourceFunction.SourceContext<DataFriday> sourceContext) throws Exception {
        super.FStream = new FileInputStream(super.dataFilePath);
        super.reader = new BufferedReader(new InputStreamReader(super.FStream, "UTF-8"));

        String line;
        long time;
        while (super.reader.ready() && (line = super.reader.readLine()) != null) {
            DataFriday diag = DataFriday.instanceFromString(line);
            if (diag == null){
                continue;
            }
            time = getEventTime(diag);
            sourceContext.collectWithTimestamp(diag, time);
            sourceContext.emitWatermark(new Watermark(time - 1));
        }

        super.reader.close();
        super.reader = null;
        super.FStream.close();
        super.FStream = null;
    }
}
