package _01_intro;

import _01_intro.data.Data;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.Serializable;

public class WordCount
{

    public static void main(String[] args) throws Exception
    {
        String START = "A"; // default starting character to look for

        // Set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        // Make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataSet<String> text = null;

        if (params.has("start")) {
            START = params.get("start");
            System.out.println("Searching for words starting with: "+START);
        }
        else {
            System.out.println("Searching for words starting with default character: "+START);
        }

        // Read the text file from the given input path
        if (params.has("input")) {
            text = env.readTextFile(params.get("input"));
            System.out.println("input:" + params.get("input"));
        } else {
            System.out.println("Executing WordCount example with default input data set.");
            System.out.println("Use --input to specify file input.");
            text = Data.getDefaultTextLineDataSet(env);
        }

        DataSet<String> filtered = text.filter(getFilterFunction(START));

        // Split up the lines in pairs <word, 1>
        DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());

        // Group by the tuple field 0, and sum up tuple field 1
        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(new int[]{0}).sum(1);

        // Emit result
        if (params.has("output")) {
            counts.writeAsCsv(params.get("output"), "\n", " ");
            // Execute program
            env.execute("WordCount Example - Words starting with "+START);
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
        }
    }

    public static FilterFunction<String> getFilterFunction(String start)  {
        return s -> s.startsWith(start);
    }


    public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>>
    {
        public Tuple2<String, Integer> map(String value)
        {
            return new Tuple2<>(value, 1);
        }
    }
}