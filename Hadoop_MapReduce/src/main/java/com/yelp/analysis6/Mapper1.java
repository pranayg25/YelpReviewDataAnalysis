package com.yelp.analysis6;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class Mapper1 extends Mapper<Object, Text, Text, IntWritable> {

	private static List<String> crusines = Arrays.asList("Afghan","African","Senegalese","South African","American","Arabian","Argentine","Armenian","Asian Fusion","Australian","Austrian","Bangladeshi","Barbeque","Basque","Belgian","Brasseries","Brzilian","British","Buffets","Burgers","Burmese","Cambodian","Caribbean","Dominican","Haitian","Puerto Rican","Trinidadian","Catalan","Chinese","Cantonese","Hainan","Shanghainese","Szechuan","Cuban","Czech","Ethiopian","Filipino","French","Mauritius","Geran","Greek","Guamanian","Halal","Hawaiian","Himalayan","Honduran","Hungarian","Iberian","Indian","Indonesian","Irish","Italian","Calabrian","Sardinian","Siilian","Tuscan","Japanese","Izakaya","Japanese Curry","Ramen","Teppanyaki","Kebab","Korean","Kosher","Laotian","Latin American","Colombian","Salvadoran","Venezuelan","Malaysian","Mediterranean","Mexican","Middle Eastern","Egyptian","Lebanese","Modern European","Mongolian","Moroccan","New Mexican Cuisine","Nicaraguan","Noodles","Pakistani","Pan Asian","Persian","Peruvian","Pizza","Polish","Portuguese","Poutineries","Russian","Salad","Sandwiches","Sandinavian","Scottish","Seafood","Singaporean","Slovakian","Soul Food","Soup","Southern","Spanish","Sri Lankan","Syrian","Taiwanese","Thai","Turkish","Ukrainian","Uzbek","Vietnamese");

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		try {

			JSONObject	jsonData = new JSONObject(value.toString());

			if(jsonData.getInt("is_open")==1) {
				if((jsonData.get("attributes")).getClass().equals(JSONArray.class)) {
					JSONArray attributes =jsonData.getJSONArray("attributes");
					for(int i=0;i<attributes.length();i++) {
						String[] attribute = attributes.getString(i).split(":");
						if(attribute[0].equals("RestaurantsPriceRange2")) {
							if(Integer.parseInt(attribute[1].trim())>=3) {
								if((jsonData.get("categories")).getClass().equals(JSONArray.class)) {
									List<String> categorys = getCategory(jsonData.getJSONArray("categories"));
									for (String cat : categorys) {
										context.write(new Text(cat), new IntWritable(1));
									}
								}							
							}
							break;
						}
					}
				}
			}
		} catch (JSONException e1) {
			e1.printStackTrace();
		}

	}

	private List<String> getCategory(JSONArray categories) throws JSONException {

		List<String> strs = new ArrayList<String>();

		for(int i = 0; i<categories.length();i++) {
			String s = (String)categories.get(i);
			if(crusines.contains(s)) {
				strs.add(s);
			}
		}
		return strs;
	}
}