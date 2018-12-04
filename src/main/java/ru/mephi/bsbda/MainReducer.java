package ru.mephi.bsbda;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


//Reducer class.
public class MainReducer extends Reducer<Text, LogInfo, Text, Text> {

    //Reducer main method.
    @Override
    public void reduce(Text key, Iterable<LogInfo> values, Context context) throws IOException, InterruptedException
    {
        double count = 0;
        int sum = 0;
		
		Text ip = new Text("");
		Text text = new Text("");

		//counting total bytes
        for (LogInfo value : values)
        {
            ++count;
            sum += value.getTotalByte();
			
			if(!value.getIp().toString().equals(""))
				ip.set(value.getIp());
        }

        //counting average bytes
        double average = sum / count;

        if(ip.toString().equals("")) {
            text = new Text(average + ";" + sum + ";");
        }
        else {
            key.set(ip);
            text = new Text(average + ";" + sum + ";");
        }

        context.write(key, text);
    }
}