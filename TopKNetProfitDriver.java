import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;

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
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            long startDateInt = Long.parseLong(conf.get("start_date"));
            long endDateInt = Long.parseLong(conf.get("end_date"));

            Date startDate = new Date(startDateInt * 1000);
            Date endDate = new Date(endDateInt * 1000);

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());

                try {
                    // Only emit if it's within the date range provided
                    String[] splitRecord = value.toString().split("\\|");

                    // if the sale line is within the date range we care about we should emit the
                    // store_sk and the profit from this sale

                    // Get the date of the sale
                    Date saleDate = new Date(Long.parseLong(splitRecord[0]) * 1000);
                    System.out.println(saleDate.toString());
                    System.out.println("-------");
                    if (saleDate.after(startDate) && saleDate.before(endDate)) {
                        System.out
                                .println("Is within range! store: " + splitRecord[7] + "   profit: " + splitRecord[22]);
                        context.write(new Text(splitRecord[7]),
                                new DoubleWritable(Double.parseDouble(splitRecord[22])));
                    }
                } catch (NumberFormatException nfe) {
                    System.err.println(value + " nfe");
                    continue;
                } catch (ArrayIndexOutOfBoundsException aiobe) {
                    System.err.println(value + "   array");
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
        FileOutputFormat.setOutputPath(job, new Path(args[4]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}