package com.lma.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
	@Override
	protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {

		ArrayList<TableBean> orderBeans = new ArrayList<>();
		TableBean pdBean = new TableBean();

		// 一组相同的key会同时进入到reduce方法中
		for (TableBean tableBean : values) {
			if (TableBean.ORDER_FLAG.equals(tableBean.getFlag())) {
				TableBean tmpTableBean = new TableBean();
				try {
					BeanUtils.copyProperties(tmpTableBean, tableBean);
				} catch (Exception e) {
					e.printStackTrace();
				}
				orderBeans.add(tmpTableBean);
			} else {
				try {
					BeanUtils.copyProperties(pdBean, tableBean);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		for (TableBean orderBean : orderBeans) {
			orderBean.setpName(pdBean.getpName());
			context.write(orderBean, NullWritable.get());
		}

	}
}
