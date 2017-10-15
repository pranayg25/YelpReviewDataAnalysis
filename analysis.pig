-- analysis 2
reviews = LOAD '/home/pranay/Downloads/yelp_dataset_challenge_round9/us_review.json' using JsonLoader('review_id:chararray,user_id:chararray,business_id:chararray, stars:float, date:chararray, text:chararray, useful:int, funny:int, cool:int,type:chararray');

formatedData = foreach reviews generate business_id,GetMonth(ToDate(date)),text;

groupformatedData = group formatedData by ($0,$1) PARALLEL 1;

countformateddata = foreach groupformatedData generate FLATTEN(group) as (business_id,month), COUNT(formatedData);

businessCategory = LOAD '/home/pranay/Downloads/SecondarySortingLab-master/analysis2_output/part-r-00000' as (business_id:chararray,category:chararray);

finalAnalysis = join countformateddata by business_id, businessCategory by business_id;

groupCategory = group finalAnalysis by ($4,$1) PARALLEL 1;

countCategory = foreach groupCategory generate FLATTEN(group) as (category,month), COUNT(finalAnalysis);

store countCategory into '/home/pranay/Downloads/pig/analysis2';


-- analysis3

data = load '/home/pranay/Downloads/SecondarySortingLab-master/analysis3_output/part-r-00000' as (city:chararray,cusine:chararray,count:int);

groupdata = group data by city;

filterdata = foreach groupdata {
	orderdata = ORDER data BY count DESC;
	top = limit orderdata 5;
	generate flatten(top);
};

store filterdata into '/home/pranay/Downloads/pig/Analysis3';

-- analysis4 

checkins = load '/home/pranay/Downloads/SecondarySortingLab-master/analysis4_1_output/part-r-00000' as (business_id:chararray,day:chararray,count:int);

businessCity = load '/home/pranay/Downloads/SecondarySortingLab-master/analysis4_2_output/part-r-00000' as (business_id:chararray,city:chararray);

joindata = join checkins by business_id, businessCity by business_id;

groupdata = group joindata by ($4,$1) PARALLEL 1;

sumdata = foreach groupdata generate FLATTEN(group) as (city,day), SUM(joindata.$2);

store sumdata into '/home/pranay/Downloads/pig/Analysis4';


-- analysis 5

data = load '/home/pranay/Downloads/SecondarySortingLab-master/analysis5_output/part-r-00000' as (city:chararray,count:int);

orderdata = ORDER data BY count DESC;

store orderdata into '/home/pranay/Downloads/pig/Analysis5';

-- analysis 6

data = load '/home/pranay/Downloads/SecondarySortingLab-master/analysis6_output/part-r-00000' as (cuzine:chararray,count:int);

orderdata = ORDER data BY count DESC;

store orderdata into '/home/pranay/Downloads/pig/Analysis6';

-- analysis 7

data1 = load '/home/pranay/Downloads/SecondarySortingLab-master/analysis7_1_output/part-r-00000' as (business_id:chararray,count:float);

data2 = load '/home/pranay/Downloads/SentimentAnalysis-master/output/part-r-00000' as (business_id:chararray,count:float);

joindata = join data1 by business_id, data2 by business_id;

store joindata into '/home/pranay/Downloads/pig/Analysis7' ;

