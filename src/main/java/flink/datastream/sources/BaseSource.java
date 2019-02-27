package flink.datastream.sources;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;

public class BaseSource<T> implements SourceFunction<T> {

    protected final String dataFilePath;
    protected final int servingSpeed;

    protected transient BufferedReader reader;
    protected transient InputStream FStream;

    public BaseSource(String dataFilePath) {
        this(dataFilePath, 1);
    }

    public BaseSource(String dataFilePath, int servingSpeedFactor) {
        this.dataFilePath = dataFilePath;
        this.servingSpeed = servingSpeedFactor;
    }


    @Override
    public void run(SourceContext<T> sourceContext) throws Exception {

    }

    @Override
    public void cancel() {
        try {
            if (this.reader != null) {
                this.reader.close();
            }
            if (this.FStream != null) {
                this.FStream.close();
            }
        } catch (IOException ioe) {
            throw new RuntimeException("Could not cancel SourceFunction", ioe);
        } finally {
            this.reader = null;
            this.FStream = null;
        }
    }
}
