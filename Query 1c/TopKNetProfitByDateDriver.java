import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Compile command:
//hadoop com.sun.tools.javac.Main TopKNetProfitByDateDriver.java

//Turn into jar file
//jar cf TopKNetProfitByDate.jar TopKNetProfitByDateDriver*.class

//Example running command:
//$HADOOP_HOME/bin/hadoop jar TopKNetProfitByDate.jar TopKNetProfitByDateDriver 10 2451520 2451771 input/40G/store_sales/store_sales.dat output/topknetprofitdate
public class TopKNetProfitByDateDriver {

    /**
     * MAPPER for job1 of TopKNetProfitByDate MapReduce query. Takes store_sales.dat
     * as input and emits ([ss_sold_date_sk ss_sold_time_sk], [net_profit]) pairs.
     */
    public static class DateProfitMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        // Sales lines
        private Text saleLine = new Text();

        /**
         * Iterate over store_sales records and emit ([ss_sold_date_sk ss_sold_time_sk],
         * [net_profit]) pairs. Filter out records with invalid date(s), net_profit or
         * ss_sold_date_sk ss_sold_time_sk.
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            long startDate = Long.parseLong(conf.get("start_date"));
            long endDate = Long.parseLong(conf.get("end_date"));

            StringTokenizer itr = new StringTokenizer(value.toString());

            // Instantiate reuasable mapper variables
            Text keyWritable = new Text();
            DoubleWritable valueWritable = new DoubleWritable();

            while (itr.hasMoreTokens()) {
                saleLine.set(itr.nextToken());

                try {
                    String[] splitRecord = value.toString().split("\\|");
                    long saleDate = Long.parseLong(splitRecord[0]);

                    // Write (ss_sold_date_sk ss_sold_time_sk, ss_net_profit) if record in date
                    // range and ss_sold_date_sk ss_sold_time_sk
                    // not empty
                    if ((saleDate >= startDate) && (saleDate <= endDate) && !splitRecord[0].equals("")) {
                        keyWritable.set(splitRecord[0]);
                        valueWritable.set(Double.parseDouble(splitRecord[22]));
                        context.write(keyWritable, valueWritable);
                    }
                } catch (NumberFormatException | ArrayIndexOutOfBoundsException ex) {
                }
            }
        }
    }

    /**
     * REDUCER for job1 of TopKNetProfitByDate MapReduce query. Takes
     * ([ss_sold_date_sk ss_sold_time_sk], [net_profit]) pairs as input and emits
     * ([ss_sold_date_sk ss_sold_time_sk], SUM([net_profit])) pairs.
     */
    public static class NetProfitReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

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
     * MAPPER for job2 of TopKNetProfitByDate MapReduce query. Takes
     * ([ss_sold_date_sk ss_sold_time_sk], SUM([net_profit])) pairs as input and
     * emits K ([ss_sold_date_sk ss_sold_time_sk], SUM([net_profit])) pairs which
     * have the largest value (net_profit sum).
     */
    public static class NetProfitMapperTopK extends Mapper<Object, Text, Text, DoubleWritable> {
        /**
         * Iterates over the (date, profit) pairs, inserts each pair into localTop,
         * which is then sorted using a Java stream to give sortedLocalTop.
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int numStores = Integer.parseInt(conf.get("num_stores"));

            // Text to hold a line of the job 1 output
            Text storeProfit = new Text();

            // localTop will hold the local pairs of the mapper.
            // sortedLocalTop will hold the pairs sorted by net profit.
            HashMap<String, Double> localTop = new HashMap<String, Double>();

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                storeProfit.set(itr.nextToken());
                String[] splitCompanyProfitLine = value.toString().split("\\s+");
                localTop.put(splitCompanyProfitLine[0], Double.parseDouble(splitCompanyProfitLine[1]));
                // Stream sort localTop on the net profit value.
                localTop = localTop.entrySet().stream()
                        .sorted(Comparator.comparing(Entry::getValue, Comparator.reverseOrder())).limit(numStores)
                        .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
            }

            // Instantiate key and value writables
            Text keyWritable = new Text();
            DoubleWritable valueWritable = new DoubleWritable();

            for (Map.Entry<String, Double> companyProfit : localTop.entrySet()) {
                keyWritable.set(companyProfit.getKey());
                valueWritable.set(companyProfit.getValue());

                context.write(keyWritable, valueWritable);
            }
        }
    }

    /**
     * REDUCER for job2 of TopKNetProfitByDateDriver MapReduce query. Takes the top
     * K ([ss_sold_date_sk ss_sold_time_sk], SUM([net_profit])) pairs from each
     * mapper and emits the overall top K pairs according to net_profit.
     */
    public static class NetProfitReducerTopK extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        HashMap<String, Double> companyProfits = new HashMap<String, Double>();

        /**
         * Puts key-value pairs into the hashmap so we can determine which pairs have
         * the largest profit
         */
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            List<DoubleWritable> combinedProfit = new ArrayList<DoubleWritable>();
            for (DoubleWritable val : values) {
                combinedProfit.add(val);
            }
            companyProfits.put(key.toString(), combinedProfit.get(0).get());
        }

        /**
         * Stream sort the hashmap contents so we can determine the dates with the
         * largest profits
         */
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int numStores = Integer.parseInt(conf.get("num_stores"));

            Text keyWritable = new Text();
            DoubleWritable valueWritable = new DoubleWritable();

            Map<String, Double> sortedCompanyProfits = companyProfits.entrySet().stream()
                    .sorted(Comparator.comparing(Entry::getValue, Comparator.reverseOrder())).limit(numStores)
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

            // This was limited previously to the max provided by the argument so we can
            // just do the whole thing and not worry about output size
            for (Map.Entry<String, Double> companyProfit : sortedCompanyProfits.entrySet()) {
                keyWritable.set(companyProfit.getKey());
                valueWritable.set(companyProfit.getValue());

                context.write(keyWritable, valueWritable);
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

        Job job = Job.getInstance(conf, "Q3, job 1 - DateProfit");
        job.setJarByClass(TopKNetProfitByDateDriver.class);
        job.setMapperClass(DateProfitMapper.class);
        job.setCombinerClass(NetProfitReducer.class);
        job.setReducerClass(NetProfitReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[3]));

        // Increase the minimum size of input splits to force fewer map jobs.
        // Allow reducer to launch alongside final map run
        FileInputFormat.setMinInputSplitSize(job, 1458607805); // 11 splits: 6/0,5/1

        FileOutputFormat.setOutputPath(job, new Path("output/topknetprofitbydatetemp"));
        job.waitForCompletion(true);

        // Enable Uber mode to reduce the intialisation overheads because the job
        // is lightweight.
        conf.setBoolean("mapreduce.job.ubertask.enable", true);

        Job job2 = Job.getInstance(conf, "Q3, job2 - TOP K");
        job2.setJarByClass(TopKNetProfitByDateDriver.class);
        job2.setMapperClass(NetProfitMapperTopK.class);
        job2.setReducerClass(NetProfitReducerTopK.class);
        job2.setNumReduceTasks(1);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job2, new Path("output/topknetprofitbydatetemp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[4]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}