package com.autentia.tutoriales;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.autentia.tutoriales.AirQualityManager.AirQualityMapper;
import com.autentia.tutoriales.AirQualityManager.AirQualityReducer;

public class AirQualityManagerTest {
	
	MapDriver<Object, Text, MeasureWritable, FloatWritable>											mapDriver;
	ReduceDriver<MeasureWritable, FloatWritable, MeasureWritable, FloatWritable>					reduceDriver;
	MapReduceDriver<Object, Text, MeasureWritable, FloatWritable, MeasureWritable, FloatWritable>	mapReduceDriver;
	
	@Before
	public void setUp() {
		AirQualityMapper mapper = new AirQualityMapper();
		AirQualityReducer reducer = new AirQualityReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("655209;1;796764372490213;804422938115889;6"));
		mapDriver.withOutput(new MeasureWritable("2014", "badajoz"), new FloatWritable(1));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws Exception {
		List<FloatWritable> values = new ArrayList<FloatWritable>();
		values.add(new FloatWritable(1));
		values.add(new FloatWritable(1));
		reduceDriver.withInput(new MeasureWritable("2014", "badajoz"), values);
		reduceDriver.withOutput(new MeasureWritable("2014", "badajoz"), new FloatWritable(1));
		reduceDriver.runTest();
	}
	
}
