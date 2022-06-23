package pagerank.impl;
import pagerank.question.PageRankReducer;
import pagerank.question.PageRankRunner;
import pagerank.question.utils.ReducePageRankWritable;
import org.apache.arrow.flatbuf.Null;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import java.io.IOException;

public class PageRankReducerImpl extends PageRankReducer{
    private static final double D = 0.85;
    @Override
    public void reduce(Text key, Iterable<ReducePageRankWritable> values,
                       Reducer<Text, ReducePageRankWritable, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        //page数
        int totalPage = context.getConfiguration().getInt(PageRankRunner.TOTAL_PAGE, 0);
        int iteration = context.getConfiguration().getInt(PageRankRunner.ITERATION, 0);
        //context.getCounter(Counter.GROUP_NAME).increment(1);
        String[] pageInfo = null;
        double sum = 0;
        for(ReducePageRankWritable value:values){
            //System.out.println("key"+key);
            String tag = value.getTag();
            if(tag.equals(ReducePageRankWritable.PAGE_INFO)){
                pageInfo = value.getData().split(" ");
                //System.out.println("page_info:"+value.getData());
            }else if(tag.equals(ReducePageRankWritable.PR_L)){
                double rank = Double.parseDouble(value.getData());
                sum = sum + rank;
                //System.out.println("page_l:"+value.getData());
            }
        }
        double rank = (1 - D) / totalPage + D * sum;
        double oldRank = Double.parseDouble(pageInfo[1]);
        double edge = rank - oldRank;
        pageInfo[1] = String.valueOf(rank);
        //最后一次迭代时或更新的差值已经小于1e-6时，输出
        StringBuilder result = new StringBuilder();

        if(Math.abs(edge) < 1e-6){
            for(String data:pageInfo){
                result.append(data).append(" ");
            }
            context.getCounter(PageRankRunner.GROUP_NAME,PageRankRunner.COUNTER_NAME).increment(1);
        } else {
            for(String data:pageInfo){
                result.append(data).append(" ");
            }
        }
        context.write(new Text(result.toString()), NullWritable.get());
    }
}
