package com.gulu.join.reduce_join;

import com.gulu.join.TableBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * join在reduce端处理，reduce压力较大
 *
 * @author chocolate
 * 2025/2/20 17:13
 */
public class JoinReduce1 extends Reducer<Text, TableBean, TableBean, NullWritable> {

    String pName;

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Reducer<Text, TableBean, TableBean, NullWritable>.Context context) throws IOException, InterruptedException {
        List<TableBean> orderList = new ArrayList<>();

        for (TableBean tableBean : values) {
            if ("order".equals(tableBean.getType())) {
                TableBean bean = new TableBean();
                try {
                    BeanUtils.copyProperties(bean,tableBean);
                    orderList.add(bean);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            } else {
                pName = tableBean.getpName();
            }
        }

        for (TableBean o : orderList) {
            o.setpName(pName);
            context.write(o, NullWritable.get());
        }

    }
}
