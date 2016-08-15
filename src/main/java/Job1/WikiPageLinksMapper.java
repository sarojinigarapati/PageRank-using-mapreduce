package Job1;

/**
 * Created by SAROJINI on 8/15/2016.
 */
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.*;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jdom2.*;
import org.jdom2.input.SAXBuilder;



public class WikiPageLinksMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String inputString = "value.toString()";
        SAXBuilder builder = new SAXBuilder();
        Reader in = new StringReader(inputString);
        try{
            Document doc = builder.build(in);
            Element root = doc.getRootElement();
            String link="";
            String pageTitle =root.getChild("title").getTextTrim() ;
            String pageText =root.getChild("revision").getChild("text").getTextTrim();
            pageTitle = pageTitle.replace(' ', '_');
            /* Pattern to extract text inside "[[ ]]" */
            Pattern pattern = Pattern.compile("\\[\\[.*?\\]\\]");
            Matcher matcher = pattern.matcher(pageText);
            String[] tempArray;
            while (matcher.find()) {
                link = matcher.group(0).substring(2, matcher.group(0).length() - 2);
                if (link.contains("[[")) {
                    tempArray = link.split("\\[\\[");
                    link = tempArray[tempArray.length - 1];
                } else if (link.contains("]]")) {
                    tempArray = link.split("\\]\\]");
                    link = tempArray[0];
                }
                /* Fetching the first page when a '|' occurs */
                int pipePos = link.indexOf('|');
                if(pipePos >= 0){
                    link = (link.substring(0, pipePos));
                }
	   			/* Replacing spaces with '_' */
                link = link.replace(' ', '_');
                /* Excluding interwiki links, section linking and table row linking */
                if ( link.indexOf(':') < 0  && link.indexOf('#') < 0 && !link.equals(pageTitle)) {
                    context.write(new Text(pageTitle + "    " + 1.0 + "    "), new Text(link));
                }
                link = "";
            }
        }catch (JDOMException ex) {
            Logger.getLogger(WikiPageLinksMapper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(WikiPageLinksMapper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}

