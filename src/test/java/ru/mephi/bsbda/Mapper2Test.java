package ru.mephi.bsbda;

import org.junit.Test;
import org.junit.Before;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

import java.io.IOException;


public class Mapper2Test {

    private MapDriver<Object, Text, Text, LogInfo> mapDriv;

    @Before
    public void setUp() {
        mapDriv = MapDriver.newMapDriver(new MainMapper2());
    }

    @Test
    public void testMainMapper() throws IOException {
        mapDriv.withInput(new LongWritable(), new Text("ip10 46.72.213.133"));
        mapDriv.withInput(new LongWritable(), new Text("ip18 178.47.189.163"));
        mapDriv.withOutput(new Text("ip10"), new LogInfo(new Text("ip10"), new Text("46.72.213.133"),"", 0));
        mapDriv.withOutput(new Text("ip18"), new LogInfo(new Text("ip18"), new Text("178.47.189.163"),"", 0));

        mapDriv.runTest();
    }
}
