package ru.mephi.bsbda;

import org.junit.Test;
import org.junit.Before;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;

import java.io.IOException;


public class Mapper1Test {

   private MapDriver<Object, Text, Text, LogInfo> mapDriv;

    @Before
    public void setUp() {
        mapDriv = MapDriver.newMapDriver(new MainMapper());
    }

    @Test
    public void testMainMapper() throws IOException {
        mapDriv.withInput(new LongWritable(), new Text("ip1198 - - [18/Dec/2015:10:58:06 +0100] \"GET /images/bg_raith.jpg HTTP/1.1\" 200 329961 \"http://www.almhuette-raith.at/\" \"Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)\" \"-\""));
        mapDriv.withInput(new LongWritable(), new Text("ip1552 - - [24/Dec/2015:18:00:42 +0100] \"GET /images/phocagallery/almhuette/thumbs/phoca_thumb_m_almhuette_raith_005.jpg HTTP/1.1\" 200 5062 \"http://www.almhuette-raith.at/index.php?option=com_phocagallery&view=category&id=1&Itemid=53\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53\" \"-\""));
        mapDriv.withOutput(new Text("ip1198"), new LogInfo(new Text("ip1198"), new Text(""),"Mozilla", 329961));
        mapDriv.withOutput(new Text("ip1552"), new LogInfo(new Text("ip1552"), new Text(""),"Mozilla", 5062));

        mapDriv.runTest();
    }
}
