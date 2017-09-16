# MapReduce
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
