import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.shaded.org.apache.http.HttpResponse;
import org.apache.hadoop.shaded.org.apache.http.client.methods.HttpGet;
import org.apache.hadoop.shaded.org.apache.http.impl.client.CloseableHttpClient;
import org.apache.hadoop.shaded.org.apache.http.impl.client.HttpClients;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AnagramSortedValuesDriver extends Configured implements Tool
{
    @Override
    public int run(String[] args) throws Exception
    {

        if (args.length != 2)
        {
            System.out.printf("Usage: %s [generic options] <input dir> <output dir>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }


    JobConf conf = new JobConf(getConf(), AnagramSortedValuesDriver.class);
        conf.setJobName(this.getClass().getName());

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setMapperClass(AnagramSortedValuesMapper.class);
        conf.setOutputValueGroupingComparator(AnagramSortedValuesOutputValueGroupingComparator.class);
        conf.setPartitionerClass(AnagramSortedValuesPartitioner.class);
        conf.setReducerClass(AnagramSortedValuesReducer.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception
    {
        cleanInput();
        int exitCode = ToolRunner.run(new AnagramSortedValuesDriver(), args);
        System.exit(exitCode);
    }

    static void cleanInput() throws Exception{
        if(new File("output").isDirectory()){
            deleteFolder(new File("output"));
        }
        File dir = new File("input");
        File[] directoryListing = dir.listFiles();
        if (directoryListing != null) {
            for (File child : directoryListing) {
                String contents = null;
                try {
                    contents = new String(Files.readAllBytes(Paths.get(child.getPath()))).replaceAll("\\p{Punct}", "");
                } catch (IOException e) {
                    e.printStackTrace();
                }
                String[] lines = contents.split("\\s+");

                CloseableHttpClient httpclient = HttpClients.createDefault();
                HttpGet httpget = new HttpGet("https://www.textfixer.com/tutorials/common-english-words-with-contractions.txt");
                HttpResponse httpresponse = httpclient.execute(httpget);
                Scanner sc = new Scanner(httpresponse.getEntity().getContent());
                StringBuffer sb = new StringBuffer();
                while(sc.hasNext()) {
                    sb.append(sc.next());
                }
                String result = sb.toString();
                result = result.replaceAll("<[^>]*>", "").toLowerCase();

                try {
                    File f1 = new File(child.getPath());

                    FileWriter fw = new FileWriter(f1);
                    BufferedWriter out = new BufferedWriter(fw);
                    for(String s : lines)
                        if(result.indexOf(s.toLowerCase())==-1) {
                            out.write(s + "\n");
                        }
                    out.flush();
                    out.close();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    static void deleteFolder(File file){
        for (File subFile : file.listFiles()) {
            if(subFile.isDirectory()) {
                deleteFolder(subFile);
            } else {
                subFile.delete();
            }
        }
        file.delete();
    }
}
