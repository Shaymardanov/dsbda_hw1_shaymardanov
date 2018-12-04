package ru.mephi.bsbda;

import org.junit.Test;
import org.junit.Before;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReducerTest {

    private ReduceDriver<Text, LogInfo, Text, Text> reducDriv;

    @Before
    public void setUp() {
        reducDriv = ReduceDriver.newReduceDriver(new MainReducer());
    }


    @Test
    public void testMainReducer() throws IOException {
        List<LogInfo> val1 = new ArrayList<LogInfo>();

        val1.add(new LogInfo(new Text(), new Text(), "", 43536));
        val1.add(new LogInfo(new Text(), new Text(), "", 86567));
        val1.add(new LogInfo(new Text(), new Text(), "", 1232));
        val1.add(new LogInfo(new Text(), new Text(), "", 75678));
        val1.add(new LogInfo(new Text(), new Text("10.128.64.13"), "", 0));
        reducDriv.withInput(new Text("ip267"), val1);

        List<LogInfo> val2 = new ArrayList<LogInfo>();

        val2.add(new LogInfo(new Text(), new Text(), "", 324));
        val2.add(new LogInfo(new Text(), new Text(), "", 346));
        val2.add(new LogInfo(new Text(), new Text(), "", 234));
        val2.add(new LogInfo(new Text(), new Text(), "", 34));
        reducDriv.withInput(new Text("ip24"), val2);

        reducDriv.withOutput(new Text("10.128.64.13"), new Text("41402.6;207013;"));
        reducDriv.withOutput(new Text("ip24"), new Text("234.5;938;"));

        reducDriv.runTest();
    }
}
