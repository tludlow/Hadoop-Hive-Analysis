import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Compile command:
//hadoop com.sun.tools.javac.Main TopKStoreProfitEmployeesDriver.java

//Turn into jar file
//jar cf TopKStoreProfitEmployees.jar TopKStoreProfitEmployeesDriver*.class

//Example running command:
//$HADOOP_HOME/bin/hadoop jar TopKStoreProfitEmployees.jar TopKStoreProfitEmployeesDriver 10 2450816 2452642 input/1G/store_sales/store_sales.dat input/1G/store/store.dat output/join_result


// NOTE: during benchmarking, some debug prints were left in the code. These are unlikely
// to dramatically affect runtime, so have been removed.
public class TopKStoreProfitEmployeesDriver {

    /**
     * MAPPER for job1 of TopKStoreProfitEmployees MapReduce query. Takes
     * store_sales.dat as input and emits ([ss_store_sk], [net_profit]) pairs.
     */
    public static class NetProfitMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        // Text object to hold a single line of store_sales.dat
        private Text saleLine = new Text();

        /**
         * Iterate over store_sales records and emit ([ss_store_sk], [net_profit])
         * pairs. Filter out records with invalid date(s), net_profit or ss_store_sk.
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            long startDate = Long.parseLong(conf.get("start_date"));
            long endDate = Long.parseLong(conf.get("end_date"));

            StringTokenizer itr = new StringTokenizer(value.toString());

            // Instantiate mapper writables
            Text keyWritable = new Text();
            DoubleWritable valueWritable = new DoubleWritable();

            while (itr.hasMoreTokens()) {
                saleLine.set(itr.nextToken());

                try {
                    String[] splitRecord = value.toString().split("\\|");
                    long saleDate = Long.parseLong(splitRecord[0]);

                    // Write (ss_store_sk, ss_net_profit) if record in date range and ss_store_sk
                    // not empty
                    if ((saleDate >= startDate) && (saleDate <= endDate) && !splitRecord[7].equals("")) {
                        keyWritable.set(splitRecord[7]);
                        valueWritable.set(Double.parseDouble(splitRecord[22]));
                        context.write(keyWritable, valueWritable);
                    }
                    // Catches exceptions where records have invalid net profit, invalid date(s), or
                    // missing attributes
                } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {}
            }
        }
    }

    /**
     * REDUCER for job1 of TopKStoreProfitEmployees MapReduce query. Takes
     * ([ss_store_sk], [net_profit]) pairs as input and emits ([ss_store_sk],
     * SUM([net_profit])) pairs.
     */
    public static class NetProfitReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        // Double writable object to hold the net_profit output.
        private DoubleWritable result = new DoubleWritable();

        /**
         * Iterates over records and sums the net_profits (the values). Emits the each
         * ss_store_sk and the corresponding sum.
         */
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /**
     * First MAPPER for job2 of the TopKStoreProfitEmployees MapReduce query. Takes
     * ([ss_store_sk], [net_profit]) pairs as input and emits ([ss_store_sk], prof +
     * [net_profit]) pairs.
     */
    public static class TagStoreProfitMapper extends Mapper<Object, Text, Text, Text> {
        private Text tupleText = new Text();

        /**
         * Iterate over ([ss_store_sk], [net_profit]) pairs, add the "prof" tag before
         * the value and emit the new pair.
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                tupleText.set(itr.nextToken());
                String[] splitRecord = value.toString().split("\\t");
                context.write(new Text(splitRecord[0]), new Text("prof\t" + splitRecord[1]));
            }
        }
    }

    /**
     * Second MAPPER for job2 of the TopKStoreProfitEmployees MapReduce query. Takes
     * stores.dat records as input and emits ([s_store_sk], emp +
     * [s_number_employees]) pairs.
     */
    public static class TagStoreEmployeeMapper extends Mapper<Object, Text, Text, Text> {
        private Text tupleText = new Text();

        /**
         * Iterate over stores.dat records, project the s_store_sk and
         * s_number_employees attributes into key and value respectively. Prepend the
         * emp tag to the value; emit the key and value.
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                // Note: this try-catch block was not present during benchmarking. This was not found
                // to be an issue, however it should be added for completeness since the stores.dat data is
                // otherwise not validated against missing attributes/delimiters.
                try {
                    tupleText.set(itr.nextToken());
                    String[] splitRecord = value.toString().split("\\|");
                    String num_employees = splitRecord[6];

                    if (!num_employees.equals("")) {
                        context.write(new Text(splitRecord[0]), new Text("emp\t" + num_employees));
                    }
                } catch (ArrayIndexOutOfBoundsException e) {}
            }
        }
    }

    /**
     * REDUCER for job2 of the TopKStoreProfitEmployees MapReduce query. Takes all
     * of the tagged pairs produced by the two mappers and combines the values of
     * the keys that match, removing the tags in the process, giving:
     * ([ss_store_sk], [net_profit] + [num_employees])
     */
    public static class ReduceSideJoinReducer extends Reducer<Text, Text, Text, Text> {
        /**
         * Iterates over each of the values for a given key and concatenates them into a
         * single value (Text object).
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String net_profit = "";
            String num_employees = "";

            for (Text val : values) {
                String splitRecord[] = val.toString().split("\\t");

                if (splitRecord[0].equals("prof")) {
                    net_profit = splitRecord[1];

                } else if (splitRecord[0].equals("emp")) {
                    num_employees = splitRecord[1];
                }
            }
            if (!num_employees.equals("")) {
                // If the net profit is missing, then replace it with 0.
                if (net_profit.equals("")) {
                    context.write(key, new Text("0\t" + num_employees));
                } else {
                    context.write(key, new Text(net_profit + "\t" + num_employees));
                }
            }
        }
    }

    /**
     * MAPPER for job3 of TopKStoreProfitEmployees MapReduce query. Takes
     * ([ss_store_sk], [net_profit] + [num_employees]) pairs as input, binds the
     * num_employees to the key, sorts the pairs by value to obtain the top K which
     * are emitted in the following form: ([ss_store_sk] + [num_employees],
     * [net_profit])
     */
    public static class ProfitEmployeesMapperTopK extends Mapper<Object, Text, Text, Text> {
        private Text table = new Text();
        HashMap<Long, String> localTop = new HashMap<Long, String>();

        /**
         * Binds the num_employees to the key to allow the records to be sorted
         * efficiently by the net_profit in the value. Outputs the top K from the stream
         * sort.
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int numStores = Integer.parseInt(conf.get("num_stores"));

            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                table.set(itr.nextToken());
                String[] splitRecord = value.toString().split("\\t");
                localTop.put(Long.parseLong(splitRecord[0]), splitRecord[1] + "\t" + splitRecord[2]);
                localTop = localTop.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey()).limit(numStores)
                        .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
            }

            for (Map.Entry<Long, String> sortedRecord : localTop.entrySet()) {
                context.write(new Text(sortedRecord.getKey().toString()), new Text(sortedRecord.getValue()));
            }
        }
    }

    /**
     * REDUCER for job3 of TopKStoreProfitEmployees MapReduce query. Takes
     * ([ss_store_sk] + [num_employees], [net_profit]) pairs as input, binds the
     * num_employees to the key, and sorts the pairs by value to obtain the top K.
     * The num_employees attribute is then re-bound to the value and the resulting
     * pairs are emitted in the following form: ([ss_store_sk], [net_profit] +
     * [num_employees])
     */
    public static class ProfitEmployeesReducerTopK extends Reducer<Text, Text, Text, Text> {
        HashMap<Long, String> companyProfits = new HashMap<Long, String>();

        /**
         * Puts key-value pairs into the HashMap so we can determine which pairs have
         * the largest profit
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> combinedProfit = new ArrayList<String>();
            for (Text val : values) {
                combinedProfit.add(val.toString());
            }
            companyProfits.put(Long.parseLong(key.toString()), combinedProfit.get(0));
        }

        /**
         * Stream sorts the HashMap to find the pairs with the top K net profit. Binds
         * the num_employees to the value and emits for each pair.
         */
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int numStores = Integer.parseInt(conf.get("num_stores"));

            Map<Long, String> sortedCompanyProfits = companyProfits.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey()).limit(numStores)
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

            // This was limited previously to the max provided by the argument so we can
            // just do the whole thing and not worry about output size
            for (Map.Entry<Long, String> companyProfit : sortedCompanyProfits.entrySet()) {
                String key = Long.toString(companyProfit.getKey());
                context.write(new Text(key),
                        new Text(companyProfit.getValue().toString()));
            }
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("num_stores", args[0]);
        conf.set("start_date", args[1]);
        conf.set("end_date", args[2]);

        // Set a slowstart parameter to delay the reducer and hence decrease the chance that 
        // the reducer starts before any failed maps are restarted.
        conf.set("mapreduce.job.reduce.slowstart.completedmaps", "0.75");

        Job job1 = Job.getInstance(conf, "Q4, job 1 - PROJECT");
        job1.setJarByClass(TopKStoreProfitEmployeesDriver.class);
        job1.setMapperClass(NetProfitMapper.class);
        job1.setCombinerClass(NetProfitReducer.class);
        job1.setReducerClass(NetProfitReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[3]));

        // Increase the minimum size of input splits to force fewer map jobs.
        // Allow reducer to launch alongside final map run
        FileInputFormat.setMinInputSplitSize(job1, 1458607805);

        FileOutputFormat.setOutputPath(job1, new Path("output/join_job1"));
        job1.waitForCompletion(true);

        // Enable Uber mode to reduce the intialisation overheads because the final
        // two jobs are lightweight.
        conf.setBoolean("mapreduce.job.ubertask.enable", true);

        Job job2 = Job.getInstance(conf, "Q4, job 2 - JOIN");
        job2.setJarByClass(TopKStoreProfitEmployeesDriver.class);
        // Setting two mappers; take input from distinct files
        MultipleInputs.addInputPath(job2, new Path("output/join_job1"), TextInputFormat.class,
                TagStoreProfitMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[4]), TextInputFormat.class, TagStoreEmployeeMapper.class);
        job2.setReducerClass(ReduceSideJoinReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job2, new Path("output/join_job2"));
        job2.waitForCompletion(true);

        // Sort the records by profit and filter top K
        Job job3 = Job.getInstance(conf, "Q4, job 3 - TOP K");
        job3.setJarByClass(TopKStoreProfitEmployeesDriver.class);
        job3.setMapperClass(ProfitEmployeesMapperTopK.class);
        job3.setReducerClass(ProfitEmployeesReducerTopK.class);
        job3.setNumReduceTasks(1);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path("output/join_job2"));
        FileOutputFormat.setOutputPath(job3, new Path(args[5]));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}