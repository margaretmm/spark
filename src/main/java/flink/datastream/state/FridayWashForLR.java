/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package flink.datastream.state;


import flink.datastream.datatypes.DataFriday;
import flink.datastream.sources.FridaySource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.lang.Math.abs;

public class FridayWashForLR {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = "D:/code/dataML/black-friday/BlackFridayNoHead.csv";
		final int servingSpeedFactor = 6000; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// operate in Event-time
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// start the data generator
		DataStream<DataFriday> DsHeart = env.addSource(
				new FridaySource(input, servingSpeedFactor));

		DataStream<String> modDataStrForLR = DsHeart
				.filter(new NoneFilter())
				.map(new mapTime()).keyBy(0)
				.flatMap(new NullFillMean())//使用T1 预测T3 因为相关性大
				.flatMap(new FridayFlatMapForLR());
		//modDataStr2.print();
		modDataStrForLR.writeAsText("./FridayDataForLR");
		// run the prediction pipeline
		env.execute("HeartData Prediction");
	}


	public static class mapTime implements MapFunction<DataFriday, Tuple2<Long, DataFriday>> {
		@Override
		public Tuple2<Long, DataFriday> map(DataFriday energy) throws Exception {
			long time = energy.eventTime.getMillis();;

			return new Tuple2<>(time, energy);
		}
	}

	public static class NoneFilter implements FilterFunction<DataFriday> {
		@Override
		public boolean filter(DataFriday sale) throws Exception {
			return IsNotNone(sale.Age) && IsNotNone(sale.Purchase) &&IsNotNone(sale.User_ID)
					 && IsNotNone(sale.Product_ID) ;
		}

		public boolean IsNotNone(Object Data){
			if (Data == null )
				return false;
			else
				return true;
		}
	}

    //使用Spark ML RF, 数据格式保存为csv常见格式
	// label 转换成一个二元变量表示
	public static class FridayFlatMapForLR implements FlatMapFunction<DataFriday, String> {

		@Override
		public void flatMap(DataFriday InputDiag, Collector<String> collector) throws Exception {
			DataFriday sale = InputDiag;

			sale.AgeParse();
			sale.GenderParse();
			sale.CityCategoryParse();
			sale.StayYearsParse();
			sale.PurchaseParse();

			collector.collect(sale.toString());
		}
	}

	public static class NullFillMean extends RichFlatMapFunction<Tuple2<Long, DataFriday> ,DataFriday> {
		//private transient ValueState<Double> heartMeanState;
		private transient ListState<Double> ProductCategoryState;
		private List<Double> meansList;

		@Override
		public void flatMap(Tuple2<Long, DataFriday>  val, Collector< DataFriday> out) throws Exception {
			Iterator<Double> modStateLst = ProductCategoryState.get().iterator();
			Double MeanProductCategory1=null;
			Double MeanProductCategory2=null;

			if(!modStateLst.hasNext()){
				MeanProductCategory1 = 8.0;
				MeanProductCategory2 = 15.0;
			}else{
				MeanProductCategory1=modStateLst.next();
				MeanProductCategory2=modStateLst.next();
			}

			DataFriday heart = val.f1;

			if(heart.Product_Category_1 == null){
				heart.Product_Category_1= Math.toIntExact(Math.round(MeanProductCategory1));
			}else if(heart.Product_Category_2 == null){
				heart.Product_Category_2= Math.toIntExact(Math.round(MeanProductCategory2));
			}else
			{
				meansList= new ArrayList<Double>();
				meansList.add(MeanProductCategory1);
				meansList.add(MeanProductCategory2);
				ProductCategoryState.update(meansList);
			}
			out.collect(heart);
		}

		@Override
		public void open(Configuration config) {
			ListStateDescriptor<Double> descriptor2 =
					new ListStateDescriptor<>(
							// state name
							"regressionModel",
							// type information of state
							TypeInformation.of(Double.class));
			ProductCategoryState = getRuntimeContext().getListState(descriptor2);
		}
	}

	public static class DataFridayExtractor extends BoundedOutOfOrdernessTimestampExtractor<DataFriday> {

		public DataFridayExtractor() {
			super(Time.seconds(60));
		}

		@Override
		public long extractTimestamp(DataFriday ride) {
				return ride.eventTime.getMillis();
		}
	}
}
