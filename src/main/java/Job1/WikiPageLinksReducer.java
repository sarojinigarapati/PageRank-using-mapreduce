package Job1;

/**
 * Created by SAROJINI on 8/15/2016.
 */
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.HashSet;

import java.io.IOException;

public class WikiPageLinksReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String output = "";
        HashSet<String> set = new HashSet<String>();
        for (Text val : values) {
            set.add(val.toString());
        }

        for(String out: set){
            output += out + "    ";
        }
        context.write(key,new Text(output));
    }
}

