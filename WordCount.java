import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

public class WordCount extends Configured  {

    public static String inputPath = "";
    public static String outputPath = "";
    public static class AnagramMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
    {

        // Input Key: Line Number
        // Input Value: Word
        //
        // Output Key: Sorted Characters of Word # Word
        // Output Value: Word
        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException
        {
            String wordString = value.toString().trim();
            char[] wordArray = wordString.toCharArray();
            Arrays.sort(wordArray);
            String wordStringSorted = String.valueOf(wordArray);
            output.collect(new Text(wordStringSorted + "#" + wordString), new Text(wordString));
        }
    }

    public static class AnagramOutputValueGroupingComparator extends WritableComparator
    {

        public AnagramOutputValueGroupingComparator()
        {
            super(Text.class, true);
        }

        // Key: Sorted Characters of Word # Word
        // Grouping: Sorted Characters of Word
        // This will ensure all the words that are anagrams of each other are grouped together
        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2)
        {
            Text compositeKey1 = (Text) wc1;
            Text compositeKey2 = (Text) wc2;

            String naturalKey1 = compositeKey1.toString().split("#")[0];
            String naturalKey2 = compositeKey2.toString().split("#")[0];

            return naturalKey1.compareTo(naturalKey2);
        }
    }
    public static class AnagramReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        ArrayList<String> listOfLists = new ArrayList<>();
        ArrayList<String> wordCount = new ArrayList<>();
        Boolean anagramFound = false;

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {
            String newKey = "";
            String newValue = "";
            while (values.hasNext()) {
                if (newKey.length() == 0) {
                    newKey = values.next().toString();
                    anagramFound = false;
                } else {
                    newValue += ", " + values.next().toString();
                    anagramFound = true;
                }
            }
            String finalValue = "";

            finalValue = Stream.of(
                    Arrays.stream((newKey + newValue).split(", "))
                            .distinct().toArray(String[]::new)).map(String::new).collect(Collectors.joining(", "));


            newValue = newKey + newValue;

            List<String> list = Arrays.asList(newValue.split(", "));
            Set<String> distinct = new HashSet<>(list);

            for (String s : distinct) {
                wordCount.add(s);
                wordCount.add("" + Collections.frequency(list, s));
            }

            if (finalValue.split(", ").length > 1) {
                newValue = Stream.of(
                        Arrays.stream(newValue.split(", "))
                                .distinct().toArray(String[]::new)).map(String::new).collect(Collectors.joining(", "));
                listOfLists.add(newValue.split(", ").length + ", " + newValue);
            }
        }

        @Override
        public void close() {
            Collections.sort(listOfLists);

            String filePath = outputPath+"/final.txt";
            try {
                File f1 = new File(filePath);

                FileWriter fw = new FileWriter(f1);
                BufferedWriter out = new BufferedWriter(fw);
                for (String val : listOfLists) {
                    String anagrams = "";
                    int count = 0;
                    for (String anagram : Arrays.copyOfRange(val.split(", "), 1, val.split(", ").length)) {
                        anagrams = anagrams + ", " + anagram + ": " + wordCount.get(wordCount.indexOf(anagram) + 1);
                        count = count + Integer.parseInt(wordCount.get(wordCount.indexOf(anagram) + 1));
                    }
                    anagrams = anagrams.substring(2);
                    out.write("Unique Anagram Count: " + val.split(", ")[0] + ", Total Anagram Count: " + count + ", Anagrams: " + "[" + anagrams + "]" + "\n");
                }
                out.flush();
                out.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }
    public static class AnagramDriver extends Configured implements Tool
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

            inputPath=args[0];
            outputPath=args[1];

            cleanInput();
            JobConf conf = new JobConf(getConf(), AnagramDriver.class);
            conf.setJobName(this.getClass().getName());
            FileInputFormat.setInputPaths(conf, new Path(args[0]));
            FileOutputFormat.setOutputPath(conf, new Path(args[1]));

            conf.setMapperClass(AnagramMapper.class);
            conf.setOutputValueGroupingComparator(AnagramOutputValueGroupingComparator.class);
            conf.setReducerClass(AnagramReducer.class);

            conf.setMapOutputKeyClass(Text.class);
            conf.setMapOutputValueClass(Text.class);

            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);

            JobClient.runJob(conf);
            return 0;
        }

        static void cleanInput() throws Exception{

            if(new File(outputPath).isDirectory()){
                deleteFolder(new File(outputPath));
            }
            File dir = new File(inputPath);
            File[] directoryListing = dir.listFiles();
            System.out.print(directoryListing[0] );
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


    public static void main(String[] args) throws Exception
    {
        int exitCode = ToolRunner.run(new AnagramDriver(), args);
        System.exit(exitCode);
    }

}
