package flink.datastream.utils;

public class dataPrepare {
    public static Double Normalization(Double x,Double max,Double min) {
        return (x - min)/max - min;
    }
}
