/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package util;

import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 *
 * @author priyankb
 */
public class Protocol {

    public static final int Year = 0;
    public static final int Month = 1;
    public static final int DayOfMonth = 2;
    public static final int DayOfWeek = 3;
    public static final int DepTime = 4;
    public static final int CRSDepTime = 5;
    public static final int ArrTime = 6;
    public static final int CRSArrTime = 7;
    public static final int UniqueCarrier = 8;
    public static final int FlightNum = 9;
    public static final int TailNum = 10;
    public static final int ActualElapsedTime = 11;
    public static final int CRSElapsedTime = 12;
    public static final int AirTime = 13;
    public static final int ArrDelay = 14;
    public static final int DepDelay = 15;
    public static final int Origin = 16;
    public static final int Dest = 17;
    public static final int Distance = 18;
    public static final int TaxiIn = 19;
    public static final int TaxiOut = 20;
    public static final int Cancelled = 21;
    public static final int CancellationCode = 22;
    public static final int Diverted = 23;
    public static final int CarrierDelay = 24;
    public static final int WeatherDelay = 25;
    public static final int NASDelay = 26;
    public static final int SecurityDelay = 27;
    public static final int LateAircraftDelay = 28;

    public static final class Code {

        public static final String A = "Carrier";
        public static final String B = "Weather";
        public static final String C = "NAS";
        public static final String D = "Security";
        public static final boolean No = false;
        public static final boolean Yes = true;

    }

    public static final class Airports {

        public static final int iata = 1;
        public static final int airport = 3;
        public static final int city = 5;
        public static final int state = 7;
        public static final int country = 9;

    }

    public static final class Carriers {

        public static final int code = 1;
        public static final int description = 3;

    }

    public static final class Airplanes {

        public static final int tailNum = 0;
        public static final int year = 8;
        public static final String NONE = "None";

    }

    public static final int getHour(int time) {
        return time / 100;
    }

    public static final <K, V extends Comparable<? super V>> SortedSet<Map.Entry<K, V>> sortMapByValue(Map<K, V> map) {
        SortedSet<Map.Entry<K, V>> sortedEntries = new TreeSet<>((Map.Entry<K, V> e1, Map.Entry<K, V> e2) -> {
            int res = e1.getValue().compareTo(e2.getValue());
            return res != 0 ? res : 1;
        });
        sortedEntries.addAll(map.entrySet());
        return sortedEntries;
    }

    public static final String getTime(int time) {
        String result = time + "00 - ";
        time++;
        result += time + "00";
        return result;
    }

    public static final String getDay(int day) {
        switch (day) {
            case 1:
                return "Monday";

            case 2:
                return "Tuesday";

            case 3:
                return "Wednesday";

            case 4:
                return "Thursday";

            case 5:
                return "Friday";

            case 6:
                return "Saturday";

            case 7:
                return "Sunday";
        }
        return null;
    }

    public static final String getMonth(int month) {
        switch (month) {
            case 1:
                return "January";

            case 2:
                return "February";

            case 3:
                return "March";

            case 4:
                return "April";

            case 5:
                return "May";

            case 6:
                return "June";

            case 7:
                return "July";

            case 8:
                return "August";

            case 9:
                return "September";

            case 10:
                return "October";

            case 11:
                return "November";

            case 12:
                return "December";
        }

        return null;
    }

//    public static final int Year = 1;
//    public static final int Month = 2;
//    public static final int DayOfMonth = 3;
//    public static final int DayOfWeek = 4;
//    public static final int DepTime = 5;
//    public static final int CRSDepTime = 6;
//    public static final int ArrTime = 7;
//    public static final int CRSArrTime = 8;
//    public static final int UniqueCarrier = 9;
//    public static final int FlightNum = 10;
//    public static final int TailNum = 11;
//    public static final int ActualElapsedTime = 12;
//    public static final int CRSElapsedTime = 13;
//    public static final int AirTime = 14;
//    public static final int ArrDelay = 15;
//    public static final int DepDelay = 16;
//    public static final int Origin = 17;
//    public static final int Dest = 18;
//    public static final int Distance = 19;
//    public static final int TaxiIn = 20;
//    public static final int TaxiOut = 21;
//    public static final int Cancelled = 22;
//    public static final int CancellationCode = 23;
//    public static final int Diverted = 24;
//    public static final int CarrierDelay = 25;
//    public static final int WeatherDelay = 26;
//    public static final int NASDelay = 27;
//    public static final int SecurityDelay = 28;
//    public static final int LateAircraftDelay = 29;
}
