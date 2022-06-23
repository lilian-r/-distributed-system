package pagerank.impl;
import pagerank.question.PageRankMapper;
import pagerank.question.utils.ReducePageRankWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankMapperImpl extends PageRankMapper{

    @Override
    public void map(LongWritable key, Text value,
                    Mapper<LongWritable, Text, Text, ReducePageRankWritable>.Context context)
            throws IOException, InterruptedException {
        super.map(key, value, context);
    }
}
