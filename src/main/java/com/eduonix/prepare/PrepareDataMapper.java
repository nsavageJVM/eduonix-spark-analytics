package com.eduonix.prepare;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/** 0    1     2    3       4       5        6      7       8     9        10       11     12    13  14  15  16  17 18  19  20     21     22     23    24
 * id, YEAR,  NI, STOPS, NETWORK, LABOREXP, STAFF, ELECEXP, KWH, TOTCOST, NARROW_T, RACK, TUNNEL, T, Q1, Q2, Q3, CT, PL, PE, PK, VIRAGE, LABOR, ELEC, CAPITAL, LNCT,
 *
 * LNQ1, LNQ2, LNQ3, LNNET, LNPL, LNPE, LNPK, LNSTOP, LNCAP, RAILROAD
 */
public class PrepareDataMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context)
            throws IOException, InterruptedException {

        String line = value.toString();


        String[] array = value.toString().split(",");

        if (!(line.length() == 0)) {
            //id,  YEAR, NETWORK, LABOREXP, STAFF, ELECEXP, KWH, TOTCOST,  LABOR,  ELEC,  CAPITAL
            context.write(new Text(array[0] ), new Text(String.format("%s|%s|%s|%s|%s|%s|%s|%s|%s|%s",
                    array[1], array[4], array[5], array[6], array[7], array[8], array[9], array[22], array[23] , array[24] ) ));
        }
    }



}
