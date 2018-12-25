package ru.mephi.bsbda;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


//Mapper2 class.
public class MainMapper2 extends Mapper<Object, Text, Text, LogInfo> {

    private static final Pattern IP_ID_PATTERN = Pattern.compile("^ip[\\d]+");  //looking for a "ip*"
                                                                    //looking for a real ip: example "10.128.64.9"
    private static final Pattern IP_PATTERN = Pattern.compile("(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})");

    Text ipId = new Text();
    Text ip = new Text();


    // Mapper method
    // Parsing string from input and processing it into useful data.
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String str = value.toString();

            Matcher ipIdMatcher = IP_ID_PATTERN.matcher(str);
            Matcher ipMatcher = IP_PATTERN.matcher(str);

			ipId.set(ipIdMatcher.find() ? ipIdMatcher.group() : "");
			ip.set(ipMatcher.find()  ? ipMatcher.group() : "");

            if(ipId.toString().equals("") || ip.toString().equals(""))
                return;

            LogInfo data = new LogInfo(ipId, ip, "", 0);

            context.write(ipId, data);
			
        } catch (NumberFormatException ex) {
            System.out.println("Exception in map function.");
        }
    }
}