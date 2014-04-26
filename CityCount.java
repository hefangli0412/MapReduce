/**
 * Created by hefangli on 4/15/14.
 */
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class CityCount {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable zero = new IntWritable(0);

        private Text word = new Text();
        JobConf conf;
        private static int count = 0;

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {

            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line , ",");

            while (tokenizer.hasMoreTokens()) {
                count++;
                tokenizer.nextToken();
                String city = tokenizer.nextToken().trim();
//                if (city.equals("(null)")) { // this is not required in this assignment
//                                             // null is treated as any other city
//                    tokenizer.nextToken();
//                    tokenizer.nextToken();
//                    continue;
//                }
                try {
                    Float latitude = Float.parseFloat(tokenizer.nextToken().trim());
                    Float longitude = Float.parseFloat(tokenizer.nextToken().trim());

                    Double x_loc = conf.getDouble("x_" + city, 0);
                    Double y_loc = conf.getDouble("y_" + city, 0);
                    double dist = Math.pow(latitude - x_loc, 2) + Math.pow(longitude - y_loc, 2);
                    if (dist <= 25) {
                        word.set(city);
                        output.collect(word, one);
                        System.out.format("set1: %s, count = %d, x_loc = %f, y_loc = %f, latitude = %f, longitude = %f, dist = %.3f%n",
                                             city, count, x_loc, y_loc, latitude, longitude, dist);
                    } else {
                        word.set(city);
                        output.collect(word, zero);
//                        System.out.format("set0: %s, count = %d, x_loc = %f, y_loc = %f, latitude = %f, longitude = %f, dist = %.3f%n",
//                                             city, count, x_loc, y_loc, latitude, longitude, dist);
                    }
                } catch (NumberFormatException e) {
                    System.err.println("DEBUG: NumberFormatExecption"); // we don't want it to crash
                }
            }
        }

        public void configure(JobConf conf) {
            this.conf = conf;
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
                           Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        JobConf conf = new JobConf(CityCount.class);
        conf.setJobName("citycount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        // the following will be distributed in parallel from master node to all working nodes
        for (int i = 0; i < file.length; i = i + 4) {
            String x_city = "x_" + file[i + 1];
            String y_city = "y_" + file[i + 1];
            Double x_loc = Double.parseDouble(file[i + 2]);
            Double y_loc = Double.parseDouble(file[i + 3]);

            conf.setDouble(x_city, x_loc);
            conf.setDouble(y_city, y_loc);
        }

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }

    // hard-coded here, considering the small-size of the file
    // otherwise, we can use distributed cache (the condensed file and pathname may be a issue)
    private static final String[] file = {
            "City0", "New York", "6967.400", "29150.000",
            "City1", "Los Angeles", "17296.400", "12625.800",
            "City2", "Chicago", "32293.800", "29658.200",
            "City4", "Philadelphia", "30959.500", "25609.100",
            "City5", "Phoenix", "18509.700", "10957.100",
            "City6", "San Antonio", "540.100", "3294.500",
            "City7", "San Diego", "13136.200", "5309.700",
            "City8", "Dallas", "5979.600", "35630.100",
            "City9", "San Jose", "16064.400", "4292.200",
            "City10", "Austin", "168.300", "321.200",
            "City11", "Jacksonville", "13620.200", "19163.100",
            "City12", "Indianapolis", "20587.600", "21689.800",
            "City13", "San Francisco", "21884.500", "5991.700",
            "City14", "Columbus", "23898.600", "16248.100",
            "City15", "Fort Worth", "12691.800", "2055.900",
            "City16", "Charlotte", "21903.200", "28233.700",
            "City17", "Detroit", "28928.900", "18738.500",
            "City18", "El Paso", "10883.400", "31573.300",
            "City19", "Memphis", "26192.100", "34454.200",
            "City20", "Boston", "33366.300", "19440.300",
            "City21", "Seattle", "5130.400", "16655.100",
            "City22", "Denver", "8482.100", "31078.300",
            "City23", "Washington", "7554.800", "28101.700",
            "City24", "Nashville", "30408.400", "35928.200",
            "City25", "Baltimore", "36032.700", "22040.700",
            "City26", "Louisville", "14144.900", "9595.300",
            "City27", "Portland", "10715.100", "30281.900",
            "City28", "Oklahoma City", "855.800", "13547.600",
            "City29", "Milwaukee", "3338.500", "24409.000",
            "City30", "Las Vegas", "2026.200", "316.800",
            "City31", "Albuquerque", "33116.600", "9944.000",
            "City32", "Tucson", "9836.200", "21190.400",
            "City33", "Fresno", "24912.800", "30190.600",
            "City34", "Sacramento", "26185.500", "17479.000",
            "City35", "Long Beach", "7401.900", "26807.000",
            "City36", "Kansas City", "16885.000", "16506.600",
            "City37", "Mesa", "34211.100", "26832.300",
            "City38", "Virginia Beach", "3902.800", "21591.900",
            "City39", "Atlanta", "13885.300", "26492.400",
            "City40", "Colorado Springs", "21949.400", "20631.600",
            "City41", "Raleigh", "13024.000", "5462.600",
            "City42", "Omaha", "8113.600", "15324.100",
            "City43", "Miami", "28938.800", "18638.400",
            "City44", "Oakland", "35682.900", "27088.600",
            "City45", "Tulsa", "12455.300", "6090.700",
            "City46", "Minneapolis", "23691.800", "17729.800",
            "City47", "Cleveland", "2290.200", "25221.900",
            "City48", "Wichita", "18195.100", "5316.300",
            "City49", "Arlington", "34226.500", "5102.900",
            "City50", "New Orleans", "32623.800", "24974.400",
            "City51", "Bakersfield", "10923.000", "15374.700",
            "City52", "Tampa", "2536.600", "34840.300",
            "City53", "Honolulu", "24624.600", "5523.100",
            "City54", "Anaheim", "31619.500", "29616.400",
            "City55", "Aurora", "20979.200", "6897.000",
            "City56", "Santa Ana", "6411.900", "29454.700",
            "City57", "Riverside", "17130.300", "5606.700",
            "City58", "Corpus Christi", "18163.200", "26384.600",
            "City59", "Pittsburgh", "14619.000", "10077.100",
            "City60", "Lexington", "20499.600", "24590.500",
            "City61", "Anchorage", "27243.700", "26020.500",
            "City62", "Stockton", "17131.400", "4434.100",
            "City63", "Cincinnati", "13257.200", "30085.000",
            "City64", "Saint Paul", "1265.000", "18635.100",
            "City65", "Toledo", "23896.400", "15362.600",
            "City66", "Newark", "3773.000", "34217.700",
            "City67", "Greensboro", "33210.100", "19807.700",
            "City68", "Plano", "12470.700", "17002.700",
            "City69", "Henderson", "13515.700", "30528.300",
            "City70", "Lincoln", "11421.300", "16439.500",
            "City71", "Buffalo", "9799.900", "35429.900",
            "City72", "Fort Wayne", "10733.800", "26643.100",
            "City73", "Jersey City","20446.800", "7064.200",
            "City74", "Chula Vista", "27440.600", "30256.600",
            "City75", "Orlando", "14333.000", "18054.300",
            "City76", "St. Petersburg", "32084.800", "990.000",
            "City77", "Norfolk", "35850.100", "20638.200",
            "City78", "Chandler", "1820.500", "19151.000",
            "City79", "Laredo", "6994.900", "30386.400",
            "City80", "Madison", "22590.700", "23702.800",
            "City81", "Durham", "7131.300", "30354.500",
            "City82", "Lubbock", "4445.100", "3962.200",
            "City83", "Winston", "26785.000", "11320.100",
            "City84", "Salem", "33919.600", "10311.400",
            "City85", "Garland", "12122.000", "5055.600",
            "City86", "Glendale", "26423.100", "30082.800",
            "City87", "Hialeah", "25518.900", "21634.800",
            "City88", "Reno", "26932.400", "9109.100",
            "City89", "Baton Rouge", "5207.400", "58.300",
            "City90", "Irvine", "2198.900", "29059.800",
            "City91", "Chesapeake", "30731.800", "7590.000",
            "City92", "Irving", "4166.800", "19939.700",
            "City93", "Scottsdale", "513.700", "4100.800",
            "City94", "Fremont", "16382.300", "27112.800",
            "City95", "Gilbert", "24731.300", "19587.700",
            "City96", "San Bernardino", "2663.100", "15741.000",
            "City97", "Boise", "7278.700", "25094.300",
            "City98", "Birmingham", "10465.400", "15739.900",
            "City99", "(null)", "8377.600", "20828.500"
    };

}


