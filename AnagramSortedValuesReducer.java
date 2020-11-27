import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class AnagramSortedValuesReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
{

    ArrayList<String> listOfLists = new ArrayList<>();
    ArrayList<String> wordCount = new ArrayList<>();
    Boolean anagramFound = false;
    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException
    {
        String newKey = "";
        String newValue = "";
        while (values.hasNext())
        {
            if (newKey.length() == 0)
            {
                newKey = values.next().toString();
                anagramFound = false;
            }
            else
            {
                newValue += ", " + values.next().toString();
                anagramFound = true;
            }
        }

        //newValue = newValue.substring(2);
        //List<String> list = Arrays.asList((newKey +", " +newValue).split(", "));
        //Collections.sort(list);
        String finalValue ="";
        //Set<String> distinct = new HashSet<>(list);

//         for (String s: distinct) {
//         finalValue = finalValue +", "+s + ": " + Collections.frequency(list, s);
//         }
        //finalValue = finalValue.substring(2);

        finalValue = Stream.of(
                Arrays.stream((newKey + newValue).split(", "))
                        .distinct().toArray(String[]::new)).map(String::new).collect(Collectors.joining(", "));


        newValue = newKey + newValue;

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
    @Override
    public void close(){
        Collections.sort(listOfLists);

        String filePath = "output/final.txt";
        try {
            File f1 = new File(filePath);

            FileWriter fw = new FileWriter(f1);
            BufferedWriter out = new BufferedWriter(fw);
            for(String val : listOfLists)
            {
                String anagrams = "";
                int count = 0;
                for(String anagram : Arrays.copyOfRange(val.split(", "), 1, val.split(", ").length)){
                    anagrams = anagrams +", " +anagram +": " + wordCount.get(wordCount.indexOf(anagram)+1);
                    count = count + Integer.parseInt(wordCount.get(wordCount.indexOf(anagram)+1));
                }
                anagrams = anagrams.substring(2);
                out.write("Unique Anagram Count: " + val.split(", ")[0] + ", Total Anagram Count: " + count+", Anagrams: "+"["+anagrams+"]"+"\n");
            }
            out.flush();
            out.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}