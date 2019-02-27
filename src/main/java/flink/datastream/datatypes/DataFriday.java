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

package flink.datastream.datatypes;

import org.joda.time.DateTime;

import java.io.Serializable;

public class DataFriday implements  Serializable {
	public String User_ID;
	public String Product_ID;
	public String Gender;
	public Integer Male;
	public Integer Female;
	public String Age;
	public Integer AgeLabel;
	public Integer AgeTeenager;//0-17
	public Integer AgeYoung1;//18-25
	public Integer AgeYoung2;//26-35
	public Integer AgeMid;//36-45
	public Integer AgeOld1;//46-50
	public Integer AgeOld2;//51-55
	public Integer AgeOld3;//55+
	public Integer Occupation;
	public String City_Category;
	public Integer CityA;
	public Integer CityB;
	public Integer CityC;
	public String Stay_In_Current_City_Years;
	public Integer StayYears1;
	public Integer StayYears2;
	public Integer StayYears3;
	public Integer StayYearsMoreThen4;
	public Integer Marital_Status;
	public Integer Product_Category_1;
	public Integer Product_Category_2;
	public Integer Product_Category_3;
	public Float Purchase;
	public Integer PurchaseLevel;
	public DateTime eventTime;

	public DataFriday() {
		this.eventTime = new DateTime();
	}

	public DataFriday(String User_ID, String Product_ID, String Gender, String Age, Integer Occupation, String City_Category, String Stay_In_Current_City_Years,
                      int Marital_Status, int Product_Category_1, int Product_Category_2, int Product_Category_3, Float Purchase) {
		this.eventTime = new DateTime();
		this.User_ID = User_ID;
		this.Product_ID = Product_ID;
		this.Gender = Gender;
		this.Age = Age;
		this.Occupation = Occupation;
		this.City_Category = City_Category;
		this.Stay_In_Current_City_Years = Stay_In_Current_City_Years;
		this.Marital_Status = Marital_Status;
		this.Product_Category_1 = Product_Category_1;
		this.Product_Category_2 = Product_Category_2;
		this.Product_Category_3 = Product_Category_3;
		this.Purchase = Purchase;
	}

	public void PurchaseParse(){
		this.PurchaseLevel= Math.toIntExact(Math.round(this.Purchase))/1000;
	}

	public void GenderParse(){
		if ("M".equals(this.Gender)){
			this.Male=1;
			this.Female=0;
		}else{
			this.Male=0;
			this.Female=1;
		}
	}
	public void CityCategoryParse(){
		this.CityA=0;
		this.CityB=0;
		this.CityC=0;
		if ("A".equals(this.City_Category)){
			this.CityA=1;
		}else if ("B".equals(this.City_Category)){
			this.CityB=1;
		}else{
			this.CityC=1;
		}
	}

	public void StayYearsParse(){
		this.StayYears1=0;
		this.StayYears2=0;
		this.StayYears3=0;
		this.StayYearsMoreThen4=0;
		if ("1".equals(this.Stay_In_Current_City_Years)){
			this.StayYears1=1;
		}else if ("2".equals(this.Stay_In_Current_City_Years)){
			this.StayYears2=1;
		}else if ("3".equals(this.Stay_In_Current_City_Years)){
			this.StayYears3=1;
		}else{
			this.StayYearsMoreThen4=1;
		}
	}

	public void AgeParse(){
		this.AgeTeenager=0;//0-17
		this.AgeYoung1=0;//18-25
		this.AgeYoung2=0;//26-35
		this.AgeMid=0;//36-45
		this.AgeOld1=0;//36-45
		this.AgeOld2=0;//36-45
		this.AgeOld3=0;//36-45
		this.AgeLabel=0;
		if ("0-17".equals(this.Age)){
			this.AgeTeenager=1;
		}else if("18-25".equals(this.Age)){
			this.AgeYoung1=1;
			this.AgeLabel=1;
		}else if("26-35".equals(this.Age)){
			this.AgeYoung2=1;
			this.AgeLabel=2;
		}else if("36-45".equals(this.Age)){
			this.AgeMid=1;
			this.AgeLabel=3;
		}else if("46-50".equals(this.Age)){
			this.AgeOld1=1;
			this.AgeLabel=4;
		}else if("51-55".equals(this.Age)){
			this.AgeOld2=1;
			this.AgeLabel=5;
		}else if("55+".equals(this.Age)){
			this.AgeOld3=1;
			this.AgeLabel=6;
		}
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(AgeLabel).append(",");
		sb.append(Male).append(",");
		sb.append(Female).append(",");
		sb.append(AgeTeenager).append(",");
		sb.append(AgeYoung1).append(",");
		sb.append(AgeYoung2).append(",");
		sb.append(AgeMid).append(",");
		sb.append(AgeOld1).append(",");
		sb.append(AgeOld2).append(",");
		sb.append(AgeOld3).append(",");
		sb.append(Occupation).append(",");
		sb.append(CityA).append(",");
		sb.append(CityB).append(",");
		sb.append(CityC).append(",");
		sb.append(StayYears1).append(",");
		sb.append(StayYears2).append(",");
		sb.append(StayYears3).append(",");
		sb.append(StayYearsMoreThen4).append(",");
		sb.append(Marital_Status).append(",");
		sb.append(Product_Category_1).append(",");
		sb.append(Product_Category_2).append(",");
		//sb.append(Product_Category_3).append(",");
        sb.append(PurchaseLevel).append(",");
		sb.append(Purchase);

		return sb.toString();
	}

	public static DataFriday instanceFromString(String line) {

		String[] tokens = line.split(",");
		if (tokens.length != 12) {
			System.out.println("#############Invalid record: " + line+"\n");
			//return null;
			//throw new RuntimeException("Invalid record: " + line);
		}

		DataFriday diag = new DataFriday();

		try {
			diag.User_ID = tokens[0].length() > 0 ? tokens[0].trim():null;
			diag.Product_ID = tokens[1].length() > 0 ? tokens[1]:null;
			diag.Gender = tokens[2].length() > 0 ? tokens[2]: null;
			diag.Age = tokens[3].length() > 0 ? tokens[3] : null;
			diag.Occupation = tokens[4].length() > 0 ? Integer.parseInt(tokens[4]) : null;
			diag.City_Category = tokens[5].length() > 0 ? tokens[5]: null;
			diag.Stay_In_Current_City_Years =tokens[6].length() > 0 ? tokens[6] : null;
			diag.Marital_Status = tokens[7].length() > 0 ? Integer.parseInt(tokens[7]) : null;
			diag.Product_Category_1 = tokens[8].length() > 0 ? Integer.parseInt(tokens[8]) : null;
			diag.Product_Category_2 = tokens[9].length() > 0 ? Integer.parseInt(tokens[9]) : null;
			diag.Product_Category_3 = tokens[10].length() > 0 ? Integer.parseInt(tokens[10]) : null;
			diag.Purchase = tokens[11].length() > 0 ? Float.parseFloat(tokens[11]) : null;
		} catch (NumberFormatException nfe) {
			throw new RuntimeException("Invalid record: " + line, nfe);
		}
		return diag;
	}

	public long getEventTime() {
		return this.eventTime.getMillis();
	}
}
