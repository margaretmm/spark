package flink.datastream.sources;


import flink.datastream.datatypes.DataHeart;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class HeartSource extends BaseSource<DataHeart> {

    public HeartSource(String dataFilePath) {
        super(dataFilePath, 1);
    }

    public HeartSource(String dataFilePath, int servingSpeedFactor) {
        super(dataFilePath,servingSpeedFactor);

    }

    public long getEventTime(DataHeart diag) {
        return diag.getEventTime();
    }

    @Override
    public void run(SourceContext<DataHeart> sourceContext) throws Exception {
        super.FStream = new FileInputStream(super.dataFilePath);
        super.reader = new BufferedReader(new InputStreamReader(super.FStream, "UTF-8"));

        String line;
        long time;
        while (super.reader.ready() && (line = super.reader.readLine()) != null) {
            DataHeart diag = DataHeart.instanceFromString(line);
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
