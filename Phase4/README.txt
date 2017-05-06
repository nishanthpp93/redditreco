In the 4th and final phase of our subreddit, the goal is to calculate the Jaccard Similarity between all the different subreddits. The mapper writes two contexts, first the key subreddit and the second subreddit it is cross-commented on. The next line written include the count of the number of times the two subreddits have been cross commented on by users. The reducer phase then uses the counts of the various subreddits and their intersection counts to calculate the Jaccard similarity. An example output:

starwars|sciencefiction 0.86576282
starwars|basketball     0.03098216

starwars and sciencefiction have a value close to 1 which means they have a large number of users that cross comment while starwars and basketball have a lower value which means there isn't much interesection of users commenting between the two subreddits.
