package com.yelp.analysis2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class Mapper1 extends Mapper<Object, Text, Text, Text> {

	private static List<String> crusines = Arrays.asList("Afghan","African","Senegalese","South African","American","Arabian","Argentine","Armenian","Asian Fusion","Australian","Austrian","Bangladeshi","Barbeque","Basque","Belgian","Brasseries","Brzilian","British","Buffets","Burgers","Burmese","Cambodian","Caribbean","Dominican","Haitian","Puerto Rican","Trinidadian","Catalan","Chinese","Cantonese","Hainan","Shanghainese","Szechuan","Cuban","Czech","Ethiopian","Filipino","French","Mauritius","Geran","Greek","Guamanian","Halal","Hawaiian","Himalayan","Honduran","Hungarian","Iberian","Indian","Indonesian","Irish","Italian","Calabrian","Sardinian","Siilian","Tuscan","Japanese","Izakaya","Japanese Curry","Ramen","Teppanyaki","Kebab","Korean","Kosher","Laotian","Latin American","Colombian","Salvadoran","Venezuelan","Malaysian","Mediterranean","Mexican","Middle Eastern","Egyptian","Lebanese","Modern European","Mongolian","Moroccan","New Mexican Cuisine","Nicaraguan","Noodles","Pakistani","Pan Asian","Persian","Peruvian","Pizza","Polish","Portuguese","Poutineries","Russian","Salad","Sandwiches","Sandinavian","Scottish","Seafood","Singaporean","Slovakian","Soul Food","Soup","Southern","Spanish","Sri Lankan","Syrian","Taiwanese","Thai","Turkish","Ukrainian","Uzbek","Vietnamese");
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		try {
			JSONObject jsonData = new JSONObject(value.toString());

			if((jsonData.get("categories")).getClass().equals(JSONArray.class)) {
				String category = getCategory(jsonData.getJSONArray("categories"));
				if(category!=null ) {
					context.write(new Text(jsonData.getString("business_id")), new Text(category) );
				}				
			}


		} catch (JSONException e1) {
			e1.printStackTrace();
		}

	}

	private String getCategory(JSONArray categories) throws JSONException {

		for(int i = 0; i<categories.length();i++) {
			String s = (String)categories.get(i);
			if(crusines.contains(s)) {
				return s;
			}
		}
		return null;
	}
}
