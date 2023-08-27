package _01_intro;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class WordCount
{
    public static void main(String[] args) throws Exception
    {
        // Set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = ParameterTool.fromArgs(args);

        // Make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // Read the text file from the given input path
        DataSet<String> text = env.readTextFile(params.get("input"));

        DataSet<String> filtered = text.filter(new FilterFunction<String>() {
            public boolean filter(String value)
            {
                return value.startsWith("N");
            }
        });

        // Split up the lines in pairs <word, 1>
        DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());

        // Group by the tuple field 0, and sum up tuple field 1
        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(new int[] { 0 }).sum(1);

        // Emit result
        if (params.has("output"))
        {
            counts.writeAsCsv(params.get("output"), "\n", " ");

            // Execute program
            env.execute("WordCount Example");
        }
    }

    public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>>
    {
        public Tuple2<String, Integer> map(String value)
        {
            return new Tuple2<>(value, 1);
        }
    }
}