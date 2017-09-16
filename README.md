# MapReduce
Objective: Produce a seperate file for each brand with brand wise list of top 10 reviewed products

Implementation:

Mapreduce concepts- Secondary sort, counters, mutiple mapreduce jobs, MultipleOutputs class 

This application produces brand wise list of top 10 reviewed products.
I have uploaded data.csv which contain test data for this application.

I have made use of Secondary sort, counters, mutiple mapreduce concepts in hadoop to implement this  use case.

1.  Counters are used for error reporting,
2.  Multiple Map Reduce jobs are used with the below intention:
    a. First job ==> Counts brandwise, productwise reviews.
    b. Second job ==> Sorts the output of 1st Mapreduce job into top 10 most reviwed product list
3. Secondary sort concept is used for sorting the results. Below classes are used:
    a. CustomWritable 
    b. SortComparator
    c. GroupComparator
    d. CustomPartitoner
 4. MutipleOutputs class to produce Brand wise seperate output files
    
Below is the command used to execute Mapreduce job:

$hadoop jar ProductRatings.jar nordStorm.productRatings.SecondarySort.ProductReviewCountDriver data/productRatingsDir output

Output:

hadoop fs -ls MR2_output
Found 5 items
-rw-r--r--   1 cloudera cloudera        422 2017-09-16 05:32 MR2_output/Amazon-r-00000
-rw-r--r--   1 cloudera cloudera         76 2017-09-16 05:32 MR2_output/Moshi-r-00001
-rw-r--r--   1 cloudera cloudera          0 2017-09-16 05:32 MR2_output/_SUCCESS
-rw-r--r--   1 cloudera cloudera          0 2017-09-16 05:32 MR2_output/part-r-00000
-rw-r--r--   1 cloudera cloudera          0 2017-09-16 05:32 MR2_output/part-r-00001

$ hadoop fs -cat MR2_output/Amazon-r-00000

1	Amazon||Amazon Tap - Alexa-Enabled Portable Bluetooth Speaker||542
2	Amazon||Amazon Fire TV||166
3	Amazon||Amazon Premium Headphones||133
4	Amazon||Fire HD 6 Tablet||87
5	Amazon||"Kindle Fire HDX 7"""||70
6	Amazon||"Kindle Fire HDX 8.9"""||43
7	Amazon||"Kindle Fire HD 7"""||41
8	Amazon||Kindle Paperwhite||39
9	Amazon||Certified Refurbished Amazon Fire TV (Previous Generation - 1st)||38
10	Amazon||Kindle Keyboard||32
$ hadoop fs -cat MR2_output/Moshi-r-00001

1	Moshi||Moshi Fire TV||10
2	Moshi||Moshi Premium Headphones||7
3	Moshi||Fire HD 6 Tablet||1
