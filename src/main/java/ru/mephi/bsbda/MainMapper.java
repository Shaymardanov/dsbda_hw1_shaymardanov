package ru.mephi.bsbda;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


//Mapper class.
public class MainMapper extends Mapper<Object, Text, Text, LogInfo> {


    private static final Pattern IP_PATTERN = Pattern.compile("^ip[\\d]+");    //looking for a "ip*"
    private static final Pattern BYTES_PATTERN = Pattern.compile("\\d{3}+ \\d+"); //looking for a "1200 23991"

    Text ipId = new Text();
    Text ip = new Text();


    // Mapper method
    // Parsing string from input and processing it into useful data.
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String str = value.toString();
        String browser = "";
        int totalByte;
        ip.set("");

        Matcher ipMatcher = IP_PATTERN.matcher(str);
        Matcher byMatcher = BYTES_PATTERN.matcher(str);


		ipId.set(ipMatcher.find() ? ipMatcher.group() : "");
		totalByte = byMatcher.find() ? Integer.parseInt(byMatcher.group(0).split(" ")[1]) : 0;

		if(ipId.toString().equals("") || totalByte == 0)
		    return;

        if (str.contains("Mozilla"))
            browser = "Mozilla";
        else if (str.contains("Opera")) {
            browser = "Opera";
        } else
            browser = "Other";

        LogInfo data = new LogInfo(ipId, ip, browser, totalByte);

        context.write(ipId, data);
    }
}