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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Compile command:
//hadoop com.sun.tools.javac.Main TopKSoldItemsDriver.java

//Turn into jar file
//jar cf TopKSoldItems.jar TopKSoldItemsDriver*.class

//Example running command:
//$HADOOP_HOME/bin/hadoop jar TopKSoldItems.jar TopKSoldItemsDriver 10 2450816 2452642 input/40G/store_sales/store_sales.dat output/topksolditems
public class TopKSoldItemsDriver {

    /**
     * MAPPER for job1 of TopKSoldItems MapReduce query. Takes store_sales.dat as
     * input and emits ([ss_item_sk], [ss_quantity]) pairs.
     */
    public static class SoldItemsMapper extends Mapper<Object, Text, Text, LongWritable> {
        // Sales lines
        private Text saleLine = new Text();

        /**
         * Iterate over store_sales records and emit ([ss_item_sk], [ss_quantity])
         * pairs. Filter out records with invalid date(s), ss_item_sk or ss_quantity.
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            long startDate = Long.parseLong(conf.get("start_date"));
            long endDate = Long.parseLong(conf.get("end_date"));

            StringTokenizer itr = new StringTokenizer(value.toString());

            Text keyWritable = new Text();
            LongWritable valueWritable = new LongWritable();

            while (itr.hasMoreTokens()) {
                saleLine.set(itr.nextToken());

                try {
                    // Only emit if it's within the date range provided
                    String[] splitRecord = value.toString().split("\\|");
                    long saleDate = Long.parseLong(splitRecord[0]);

                    // Write (ss_item_sk, ss_quantity) if record in date range and
                    // ss_item_sk/ss_quantity not empty
                    if ((saleDate >= startDate) && (saleDate <= endDate) && !splitRecord[2].equals("")) {
                        keyWritable.set(splitRecord[2]);
                        valueWritable.set(Long.parseLong(splitRecord[10]));

                        context.write(keyWritable, valueWritable);
                    }
                } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                }
            }
        }
    }

    /**
     * REDUCER for job1 of TopKSoldItems MapReduce query. Takes ([ss_item_sk],
     * [ss_quantity]) pairs as input and emits ([ss_item_sk], SUM([ss_quantity]))
     * pairs.
     */
    public static class SoldItemsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        // Long writable object to hold the quantity output.
        private LongWritable result = new LongWritable();

        /**
         * Iterates over records and sums the ss_quantity (the values). Emits the each
         * ss_item_sk and the quantity corresponding sum.
         */
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /**
     * MAPPER for job2 of TopKSoldItems MapReduce query. Takes ([ss_item_sk],
     * SUM([ss_quantity])) pairs as input and emits K ([ss_item_sk],
     * SUM([ss_quantity])) pairs which have the largest value (ss_quantity sum).
     */
    public static class SoldItemsMapperTopK extends Mapper<Object, Text, Text, LongWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int numItems = Integer.parseInt(conf.get("num_items"));

            // Text object hold a single line of job1 output
            Text itemQuantity = new Text();

            // localTop will hold the local pairs of the mapper.
            // sortedLocalTop will hold the pairs sorted by quantity.
            HashMap<String, Long> localTop = new HashMap<String, Long>();

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                itemQuantity.set(itr.nextToken());
                String[] splitItemSalesLine = value.toString().split("\\s+");
                localTop.put(splitItemSalesLine[0], Long.parseLong(splitItemSalesLine[1]));
                // Stream sort localTop on the quantity value.
                localTop = localTop.entrySet().stream()
                        .sorted(Comparator.comparing(Entry::getValue, Comparator.reverseOrder())).limit(numItems)
                        .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
            }

            // Instantiate key and value writables
            Text keyWritable = new Text();
            LongWritable valueWritable = new LongWritable();

            // Emit each entry in the locally sorted quantity list
            for (Map.Entry<String, Long> itemSaleCount : localTop.entrySet()) {
                keyWritable.set(itemSaleCount.getKey());
                valueWritable.set(itemSaleCount.getValue());
                context.write(keyWritable, valueWritable);
            }
        }
    }

    /**
     * REDUCER for job2 of TopKSoldItems MapReduce query. Takes the top K
     * ([ss_item_sk], SUM([ss_quantity])) pairs from each mapper and emits the
     * overall top K pairs according to ss_quantity.
     */
    public static class TopKNetItemsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        HashMap<String, Long> itemSales = new HashMap<String, Long>();

        /**
         * Puts key-value pairs into the hashmap so we can determine which pairs have
         * the most sold items
         */
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            List<LongWritable> combinedQuantity = new ArrayList<LongWritable>();
            for (LongWritable val : values) {
                combinedQuantity.add(val);
            }

            itemSales.put(key.toString(), combinedQuantity.get(0).get());
        }

        /**
         * Stream sort the hashmap contents so we can determine the item with the most
         * items sold
         */
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int numItems = Integer.parseInt(conf.get("num_items"));

            Text keyWritable = new Text();
            LongWritable valueWritable = new LongWritable();

            Map<String, Long> sortedItemSaleCounts = itemSales.entrySet().stream()
                    .sorted(Comparator.comparing(Entry::getValue, Comparator.reverseOrder())).limit(numItems)
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

            // This was limited previously to the max provided by the argument so we can
            // just do the whole thing and not worry about output size
            for (Map.Entry<String, Long> itemSaleCount : sortedItemSaleCounts.entrySet()) {
                keyWritable.set(itemSaleCount.getKey());
                valueWritable.set(itemSaleCount.getValue());

                context.write(keyWritable, valueWritable);
            }
        }

    }

    // Driver
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("num_items", args[0]);
        conf.set("start_date", args[1]);
        conf.set("end_date", args[2]);

        // Set a slowstart parameter to delay the reducer and hence decrease the chance that 
        // the reducer starts before any failed maps are restarted.
        conf.set("mapreduce.job.reduce.slowstart.completedmaps", "0.75");

        Job job1 = Job.getInstance(conf, "Q2, job 1 - ItemSales");
        job1.setJarByClass(TopKSoldItemsDriver.class);
        job1.setMapperClass(SoldItemsMapper.class);
        job1.setCombinerClass(SoldItemsReducer.class);
        job1.setReducerClass(SoldItemsReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        // Increase the minimum size of input splits to force fewer map jobs.
        // Allow reducer to launch alongside final map run
        FileInputFormat.setMinInputSplitSize(job1, 1458607805); // 11 splits: 6/0,5/1

        FileInputFormat.addInputPath(job1, new Path(args[3]));
        FileOutputFormat.setOutputPath(job1, new Path("output/topksolditemstemp"));
        FileOutputFormat.setCompressOutput(job1, true);
        job1.waitForCompletion(true);

        // Enable Uber mode to reduce the intialisation overheads because the job
        // is lightweight.
        conf.setBoolean("mapreduce.job.ubertask.enable", true);

        Job job2 = Job.getInstance(conf, "Q2, job 2 - TOP K");
        job2.setJarByClass(TopKSoldItemsDriver.class);
        job2.setMapperClass(SoldItemsMapperTopK.class);
        job2.setReducerClass(TopKNetItemsReducer.class);
        job2.setNumReduceTasks(1);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job2, new Path("output/topksolditemstemp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[4]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}