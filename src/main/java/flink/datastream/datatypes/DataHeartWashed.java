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

import java.io.Serializable;

/**
 * A TaxiRide is a taxi ride event. There are two types of events, a taxi ride start event and a
 * taxi ride end event. The isStart flag specifies the type of the event.
 *
 * A TaxiRide consists of
 * - the rideId of the event which is identical for start and end record
 * - the type of the event (start or end)
 * - the time of the event
 * - the longitude of the start location
 * - the latitude of the start location
 * - the longitude of the end location
 * - the latitude of the end location
 * - the passengerCnt of the ride
 * - the taxiId
 * - the driverId
 *
 */
public class DataHeartWashed implements  Serializable {
	public Integer ageTeenage;
	public Integer ageYoung;
	public Integer ageMid;
	public Integer ageOld;
	public Integer chestPain0;
	public Integer chestPain1;
	public Integer chestPain2;
	public Integer chestPain3;
	public Integer electrocardiographic0;
	public Integer electrocardiographic1;
	public Integer electrocardiographic2;
	public Integer slope0;
	public Integer slope1;
	public Integer slope2;
	public Integer thal0;
	public Integer thal1;
	public Integer thal2;
	public Integer thal3;

	public DataHeartWashed() {
		this.ageTeenage = 0;
		this.ageYoung = 0;
		this.ageMid = 0;
		this.ageOld = 0;
		this.chestPain0 = 0;
		this.chestPain1 = 0;
		this.chestPain2 = 0;
		this.chestPain3 = 0;
		this.electrocardiographic0 = 0;
		this.electrocardiographic1 = 0;
		this.electrocardiographic2 = 0;
		this.slope0 = 0;
		this.slope1 = 0;
		this.slope2 = 0;
		this.thal0 = 0;
		this.thal1 = 0;
		this.thal2 = 0;
		this.thal3 = 0;
	}
}
