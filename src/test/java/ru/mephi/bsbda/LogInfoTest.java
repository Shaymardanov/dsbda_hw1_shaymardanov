package ru.mephi.bsbda;

import org.junit.Test;

import org.apache.hadoop.io.Text;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;


public class LogInfoTest {

    private static final Text ID = new Text("ip22");
    private static final Text ip = new Text("10.128.64.5");
    private static final String browser = "Opera";
    private static final Integer totalByte = 66;

    @Test
    public void EmptyConstructorTest() {
        LogInfo test = new LogInfo();
        Integer Byte = 0;

        assertEquals("", test.getIpId().toString());
        assertEquals("", test.getIp().toString());
        assertEquals("", test.getBrowser());
        assertEquals(Byte, test.getTotalByte());
    }

    @Test
    public void FullConstructorTest() {
        LogInfo test = new LogInfo(ID, ip, browser, totalByte);

        assertEquals(ID, test.getIpId());
        assertEquals(ip, test.getIp());
        assertEquals(browser, test.getBrowser());
        assertEquals(totalByte, test.getTotalByte());
    }

    @Test
    public void hashCodeTest() {
        Text ID2 = new Text("ip33");

        LogInfo test1 = new LogInfo(ID, ip, browser, totalByte);
        LogInfo test2 = new LogInfo(ID2, ip, "Chrome", totalByte);
        assertThat(test1.hashCode(), not(test2.hashCode()));
    }

    @Test
    public void EqualsTest() {
        Text ID2 = new Text("ip33");
        LogInfo test1 = new LogInfo(ID, ip, browser, totalByte);
        LogInfo test2 = new LogInfo(ID2, ip, "Chrome", 45);
        LogInfo test3 = new LogInfo(ID, ip, browser, totalByte);
        LogInfo test4 = test1;
        assertEquals(false, test1.equals("Some object"));
        assertEquals(false, test1.equals(test2));
        assertEquals(true, test1.equals(test3));
        assertEquals(true, test1.equals(test4));
        assertEquals(true, test1.equals(test1));
    }

    @Test
    public void compareToTest() {
        Text ID2 = new Text("ip33");
        LogInfo test1 = new LogInfo(ID, ip, browser, totalByte);
        LogInfo test2 = new LogInfo(ID2, ip, "Chrome", totalByte);
        LogInfo test3 = new LogInfo(ID, ip, browser, totalByte);
        assertEquals(-1, test1.compareTo(test2));
        assertEquals(1, test2.compareTo(test1));
        assertEquals(0, test1.compareTo(test3));
        assertEquals(0, test1.compareTo(test1));
    }
}
