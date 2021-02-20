import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;
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
//hadoop com.sun.tools.javac.Main TopKNetProfitDriver.java

//Turn into jar file
//jar cf TopKNetProfit.jar TopKNetProfitDriver*.class

//Example running command:
//$HADOOP_HOME/bin/hadoop jar TopKNetProfit.jar TopKNetProfitDriver 10 0 1613332438 input/1G/store_sales/store_sales.dat output/topknetprofit
public class TopKNetProfitDriver {
    // Mapper
    public static class NetProfitMapper extends Mapper<Object, Text, Text, DoubleWritable> {
        // Sales lines
        private Text saleLine = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            long startDateInt = Long.parseLong(conf.get("start_date"));
            long endDateInt = Long.parseLong(conf.get("end_date"));

            Date startDate = new Date(startDateInt * 1000);
            Date endDate = new Date(endDateInt * 1000);

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                saleLine.set(itr.nextToken());

                try {
                    // Only emit if it's within the date range provided
                    String[] splitRecord = value.toString().split("\\|");

                    // if the sale line is within the date range we care about we should emit the
                    // store_sk and the profit from this sale

                    // Get the date of the sale
                    Date saleDate = new Date(Long.parseLong(splitRecord[0]) * 1000);
                    if (saleDate.after(startDate) && saleDate.before(endDate) && !splitRecord[7].equals("")) {
                        context.write(new Text(splitRecord[7]),
                                new DoubleWritable(Double.parseDouble(splitRecord[22])));
                    }
                } catch (NumberFormatException nfe) {
                    // System.err.println(value + " nfe");
                    continue;
                } catch (ArrayIndexOutOfBoundsException aiobe) {
                    // System.err.println(value + " array");
                }
            }
        }
    }

    // Reducer / combiner
    public static class NetProfitReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /**
     * Second round
     */

    public static class NetProfitMapperTopK extends Mapper<Object, Text, Text, DoubleWritable> {
        // store and profit lines
        private Text storeProfit = new Text();

        HashMap<String, Double> localTop = new HashMap<String, Double>();
        Map<String, Double> sortedLocalTop;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int numStores = Integer.parseInt(conf.get("num_stores"));

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                storeProfit.set(itr.nextToken());

                String[] splitCompanyProfitLine = value.toString().split("\\s+");
                // System.out.println(splitCompanyProfitLine[0] + " --- " +
                // splitCompanyProfitLine[1]);

                localTop.put(splitCompanyProfitLine[0], Double.parseDouble(splitCompanyProfitLine[1]));
                if (localTop.size() > numStores) {
                    sortedLocalTop = localTop.entrySet().stream()
                            .sorted(Comparator.comparing(Entry::getValue, Comparator.reverseOrder())).limit(numStores)
                            .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (e1, e2) -> e1,
                                    LinkedHashMap::new));
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int numStores = Integer.parseInt(conf.get("num_stores"));

            if (localTop.size() < numStores) {
                sortedLocalTop = localTop.entrySet().stream()
                        .sorted(Comparator.comparing(Entry::getValue, Comparator.reverseOrder())).limit(numStores)
                        .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
            }
            for (Map.Entry<String, Double> companyProfit : sortedLocalTop.entrySet()) {
                context.write(new Text(companyProfit.getKey()), new DoubleWritable(companyProfit.getValue()));
            }

        }
    }

    // Reducer / combiner
    public static class NetProfitReducerTopK extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        HashMap<String, Double> companyProfits = new HashMap<String, Double>();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            List<DoubleWritable> combinedProfit = new ArrayList<DoubleWritable>();
            for (DoubleWritable val : values) {
                combinedProfit.add(val);
            }

            companyProfits.put(key.toString(), combinedProfit.get(0).get());
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int numStores = Integer.parseInt(conf.get("num_stores"));

            Map<String, Double> sortedCompanyProfits = companyProfits.entrySet().stream()
                    .sorted(Comparator.comparing(Entry::getValue, Comparator.reverseOrder())).limit(numStores)
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

            // This was limited previously to the max provided by the argument so we can
            // just do the whole thing and not worry about output size
            for (Map.Entry<String, Double> companyProfit : sortedCompanyProfits.entrySet()) {
                context.write(new Text(companyProfit.getKey()), new DoubleWritable(companyProfit.getValue()));
            }
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("num_stores", args[0]);
        conf.set("start_date", args[1]);
        conf.set("end_date", args[2]);

        Job job = Job.getInstance(conf, "top k net profit");
        job.setJarByClass(TopKNetProfitDriver.class);
        job.setMapperClass(NetProfitMapper.class);
        job.setCombinerClass(NetProfitReducer.class);
        job.setReducerClass(NetProfitReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[3]));
        FileOutputFormat.setOutputPath(job, new Path("output/topknetprofittemp"));
        job.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "top k net profit second cycle");
        job2.setJarByClass(TopKNetProfitDriver.class);
        job2.setMapperClass(NetProfitMapperTopK.class);
        // job2.setCombinerClass(NetProfitReducerTopK.class);
        job2.setReducerClass(NetProfitReducerTopK.class);
        job2.setNumReduceTasks(1);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job2, new Path("output/topknetprofittemp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[4]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}