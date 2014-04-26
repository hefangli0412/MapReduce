
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class People_City {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
		private final static IntWritable zero = new IntWritable(0);
        private Text its = new Text();
        static class Location{
            double x;
            double y;
            Location(double x, double y){
                this.x = x;
                this.y = y;
            }
        }
        private static HashMap<String, Location> CityName = new HashMap<String,Location>();
        Map(){
            CityName.put("New York", new Location(6967.4, 29150));
            CityName.put("Los Angeles", new Location(17296.4, 12625.8));
            CityName.put("Chicago", new Location(32293.8, 29658.2));
            CityName.put("Houston", new Location(26910.4, 6275.5));
            CityName.put("Philadelphia", new Location(30959.5, 25609.1));
            CityName.put("Phoenix", new Location(18509.7, 10957.1));
            CityName.put("San Antonio", new Location(540.1, 3294.5));
            CityName.put("San Diego", new Location(13136.2, 5309.7));
            CityName.put("Dallas", new Location(5979.6, 35630.1));
            CityName.put("San Jose", new Location(16064.4, 4292.2));
            CityName.put("Austin", new Location(168.3, 321.2));
            CityName.put("Jacksonville", new Location(13620.2, 19163.1));
            CityName.put("Indianapolis", new Location(20587.6, 21689.8));
            CityName.put("San Francisco", new Location(21884.5, 5991.7));
            CityName.put("Columbus", new Location(23898.6, 16248.1));
            CityName.put("Fort Worth", new Location(12691.8, 2055.9));
            CityName.put("Charlotte", new Location(21903.2, 28233.7));
            CityName.put("Detroit", new Location(28928.9, 18738.5));
            CityName.put("El Paso", new Location(10883.4, 31573.3));
            CityName.put("Memphis", new Location(26192.1, 34454.2));
            CityName.put("Boston", new Location(33366.3, 19440.3));
            CityName.put("Seattle", new Location(5130.4, 16655.1));
            CityName.put("Denver", new Location(8482.1, 31078.3));
            CityName.put("Washington", new Location(7554.8, 28101.7));
            CityName.put("Nashville", new Location(30408.4, 35928.2));
            CityName.put("Baltimore", new Location(36032.7, 22040.7));
            CityName.put("Louisville", new Location(14144.9, 9595.3));
            CityName.put("Portland", new Location(10715.1, 30281.9));
            CityName.put("Oklahoma City", new Location(855.8, 13547.6));
            CityName.put("Milwaukee", new Location(3338.5, 24409));
            CityName.put("Las Vegas", new Location(2026.2, 316.8));
            CityName.put("Albuquerque", new Location(33116.6, 9944));
            CityName.put("Tucson", new Location(9836.2, 21190.4));
            CityName.put("Fresno", new Location(24912.8, 30190.6));
            CityName.put("Sacramento", new Location(26185.5, 17479));
            CityName.put("Long Beach", new Location(7401.9, 26807));
            CityName.put("Kansas City", new Location(16885, 16506.6));
            CityName.put("Mesa", new Location(34211.1, 26832.3));
            CityName.put("Virginia Beach", new Location(3902.8, 21591.9));
            CityName.put("Atlanta", new Location(13885.3, 26492.4));
            CityName.put("Colorado Springs", new Location(21949.4, 20631.6));
            CityName.put("Raleigh", new Location(13024, 5462.6));
            CityName.put("Omaha", new Location(8113.6, 15324.1));
            CityName.put("Miami", new Location(28938.8, 18638.4));
            CityName.put("Oakland", new Location(35682.9, 27088.6));
            CityName.put("Tulsa", new Location(12455.3, 6090.7));
            CityName.put("Minneapolis", new Location(23691.8, 17729.8));
            CityName.put("Cleveland", new Location(2290.2, 25221.9));
            CityName.put("Wichita", new Location(18195.1, 5316.3));
            CityName.put("Arlington", new Location(34226.5, 5102.9));
            CityName.put("New Orleans", new Location(32623.8, 24974.4));
            CityName.put("Bakersfield", new Location(10923, 15374.7));
            CityName.put("Tampa", new Location(2536.6, 34840.3));
            CityName.put("Honolulu", new Location(24624.6, 5523.1));
            CityName.put("Anaheim", new Location(31619.5, 29616.4));
            CityName.put("Aurora", new Location(20979.2, 6897));
            CityName.put("Santa Ana", new Location(6411.9, 29454.7));
            CityName.put("Riverside", new Location(17130.3, 5606.7));
            CityName.put("Corpus Christi", new Location(18163.2, 26384.6));
            CityName.put("Pittsburgh", new Location(14619, 10077.1));
            CityName.put("Lexington", new Location(20499.6, 24590.5));
            CityName.put("Anchorage", new Location(27243.7, 26020.5));
            CityName.put("Stockton", new Location(17131.4, 4434.1));
            CityName.put("Cincinnati", new Location(13257.2, 30085));
            CityName.put("Saint Paul", new Location(1265, 18635.1));
            CityName.put("Toledo", new Location(23896.4, 15362.6));
            CityName.put("Newark", new Location(3773, 34217.7));
            CityName.put("Greensboro", new Location(33210.1, 19807.7));
            CityName.put("Plano", new Location(12470.7, 17002.7));
            CityName.put("Henderson", new Location(13515.7, 30528.3));
            CityName.put("Lincoln", new Location(11421.3, 16439.5));
            CityName.put("Buffalo", new Location(9799.9, 35429.9));
            CityName.put("Fort Wayne", new Location(10733.8, 26643.1));
            CityName.put("Jersey City", new Location(20446.8, 7064.2));
            CityName.put("Chula Vista", new Location(27440.6, 30256.6));
            CityName.put("Orlando", new Location(14333, 18054.3));
            CityName.put("St. Petersburg", new Location(32084.8, 990));
            CityName.put("Norfolk", new Location(35850.1, 20638.2));
            CityName.put("Chandler", new Location(1820.5, 19151));
            CityName.put("Laredo", new Location(6994.9, 30386.4));
            CityName.put("Madison", new Location(22590.7, 23702.8));
            CityName.put("Durham", new Location(7131.3, 30354.5));
            CityName.put("Lubbock", new Location(4445.1, 3962.2));
            CityName.put("Winston", new Location(26785, 11320.1));
            CityName.put("Salem", new Location(33919.6, 10311.4));
            CityName.put("Garland", new Location(12122, 5055.6));
            CityName.put("Glendale", new Location(26423.1, 30082.8));
            CityName.put("Hialeah", new Location(25518.9, 21634.8));
            CityName.put("Reno", new Location(26932.4, 9109.1));
            CityName.put("Baton Rouge", new Location(5207.4, 58.3));
            CityName.put("Irvine", new Location(2198.9, 29059.8));
            CityName.put("Chesapeake", new Location(30731.8, 7590));
            CityName.put("Irving", new Location(4166.8, 19939.7));
            CityName.put("Scottsdale", new Location(513.7, 4100.8));
            CityName.put("Fremont", new Location(16382.3, 27112.8));
            CityName.put("Gilbert", new Location(24731.3, 19587.7));
            CityName.put("San Bernardino", new Location(2663.1, 15741));
            CityName.put("Boise", new Location(7278.7, 25094.3));
            CityName.put("Birmingham", new Location(10465.4, 15739.9));
            CityName.put("(null)", new Location(8377.6, 20828.5));
        }

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String []location = line.split(", ",4);
            Location reference_location = CityName.get(location[1]);
            double x = Double.parseDouble(location[2])-reference_location.x;
            double y = Double.parseDouble(location[3])-reference_location.y;
            if(Math.sqrt(x*x + y*y) <= 5.0){
                its.set(location[1]);
                output.collect(its,one);
            }
			else {
				its.set(location[1]);
                output.collect(its,zero);
			}
        }


    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(People_City.class);
        conf.setJobName("People_City");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }

}
