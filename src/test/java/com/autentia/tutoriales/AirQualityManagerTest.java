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
		/***
		 * 	01/01/1997;    1.2;12;33;63;56;;;;19;ÁVILA;Ávila
			02/01/1997;    1.3;15;35;59;47;;;;17;ÁVILA;Ávila
			03/01/1997;    1.5;18;43;54;65;;;;19;ÁVILA;Ávila
			04/01/1997;    1.6;56;73;50;74;;;;22;ÁVILA;Ávila
			05/01/1997;    1.4;11;33;63;54;;;;18;ÁVILA;Ávila
			06/01/1997;    1.6;28;46;56;60;;;;20;ÁVILA;Ávila
			07/01/1997;    1.5;19;41;58;47;;;;23;ÁVILA;Ávila

		 * */
		mapDriver.withInput(new LongWritable(), new Text("01/01/1997;1.2;12;33;63;56;;;;19;ÁVILA;Ávila"));
		mapDriver.withOutput(new MeasureWritable("1997", "ÁVILA"), new FloatWritable(Float.valueOf("1.2")));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws Exception {
		List<FloatWritable> values = new ArrayList<FloatWritable>();
		values.add(new FloatWritable(1));
		values.add(new FloatWritable(1));
		reduceDriver.withInput(new MeasureWritable("01/01/2014", "badajoz"), values);
		reduceDriver.withOutput(new MeasureWritable("01/01/2014", "badajoz"), new FloatWritable(1));
		reduceDriver.runTest();
	}
	
}
