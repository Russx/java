import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AnagramFinder {

    public static String inputPath = "";
    public static String outputPath = "";
    public static class AnagramsMapper extends Mapper<Object, Text, Text, Text> {
        String[] stopWords = {"meat"};

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer iterable = new StringTokenizer(value.toString());

            while (iterable.hasMoreTokens()) {
                String word = iterable.nextToken().toLowerCase();
                char[] arr = word.toCharArray();
                Arrays.sort(arr);
                String wordKey = new String(arr);
                for (String stopWord : stopWords) {
                    if (!word.equals(stopWord)) {
                        context.write(new Text(wordKey), new Text(word));
                    }
                }
            }
        }
    }

    public static class AnagramsReducer extends Reducer<Text, Text, Text, Text> {
        ArrayList<String> listOfLists = new ArrayList<>();
        ArrayList<String> wordCount = new ArrayList<>();
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String newValue = null;
            for (Text val : values) {
                if (newValue == null) {
                    newValue = val.toString();
                }
                else {
                    newValue = newValue + ", " + val.toString();
                }
            }
            String finalValue ="";

            finalValue = Stream.of(
                    Arrays.stream((newValue).split(", "))
                            .distinct().toArray(String[]::new)).map(String::new).collect(Collectors.joining(", "));
            List<String> list = Arrays.asList(newValue.split(", "));
            Set<String> distinct = new HashSet<>(list);
            for (String s: distinct) {
                wordCount.add(s);
                wordCount.add(""+Collections.frequency(list, s));
            }

            if (finalValue.split(", ").length > 1)
            {
                newValue = Stream.of(
                        Arrays.stream(newValue.split(", "))
                                .distinct().toArray(String[]::new)).map(String::new).collect(Collectors.joining(", "));
                listOfLists.add(newValue.split(", ").length +", "+newValue);
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            Collections.sort(listOfLists);
            for(String val : listOfLists)
            {
                String anagrams = "";
                int count = 0;
                for(String anagram : Arrays.copyOfRange(val.split(", "), 1, val.split(", ").length)){
                    anagrams = anagrams +", " +anagram +": " + wordCount.get(wordCount.indexOf(anagram)+1);
                    count = count + Integer.parseInt(wordCount.get(wordCount.indexOf(anagram)+1));
                }
                anagrams = anagrams.substring(2);
                context.write(new Text("Unique Anagram Count: " + val.split(", ")[0] ), new Text( ", Total Anagram Count: " + count+", Anagrams: "+"["+anagrams+"]"));
            }

        }
    }
    public static void main(String[] args) throws Exception {
        inputPath=args[0];
        outputPath=args[1];
        cleanInput();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Anagram");

        job.setJarByClass(AnagramFinder.class);
        job.setMapperClass(AnagramsMapper.class);
        job.setReducerClass(AnagramsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    static void cleanInput() throws Exception{

        if(new File(outputPath).isDirectory()){
            deleteFolder(new File(outputPath));
        }
        File dir = new File(inputPath);
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
