# MapReduce
Objective: Produce a seperate file for each brand with brand wise list of top 10 reviewed products

Implementation:

Mapreduce concepts- Secondary sort, counters, mutiple mapreduce jobs

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
    
Below is the command used to execute Mapreduce job:

hadoop jar ProductRatings.jar nordStorm.productRatings.SecondarySort.ProductReviewCountDriver data/productRatingsDir output

Output:

hadoop fs -cat output2/part-r-00000
0	Amazon||Amazon Tap - Alexa-Enabled Portable Bluetooth Speaker||542
1	Amazon||Amazon Fire TV||166
2	Amazon||Amazon Premium Headphones||133
3	Amazon||Fire HD 6 Tablet||87
4	Amazon||"Kindle Fire HDX 7"""||70
5	Amazon||"Kindle Fire HDX 8.9"""||43
6	Amazon||"Kindle Fire HD 7"""||41
7	Amazon||Kindle Paperwhite||39
8	Amazon||Certified Refurbished Amazon Fire TV (Previous Generation - 1st)||38
9	Amazon||Kindle Keyboard||32
10	Amazon||All-New Amazon Fire 7 Tablet Case (7th Generation||27

hadoop fs -cat output2/part-r-00001
0	Moshi||Moshi Anti-Glare No Bubble Screen Protector for the Fire Phone||12
1	Moshi||Moshi Fire TV||10
2	Moshi||Moshi Premium Headphones||7
3	Moshi||Fire HD 6 Tablet||1
