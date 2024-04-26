db.createView(
   "most_liked_posts",
   [
     { $unionWith: "Tweets" },
     { $unionWith: "Retweets" },
     { $unionWith: "Quoted_Tweets" },
     { $sort: { Likes_Count: -1 } },
     { $limit: 10 }
   ]
)

db.createView(
   "most_quoted_posts",
   [
     { $unionWith: "Tweets" },
     { $unionWith: "Retweets" },
     { $unionWith: "Quoted_Tweets" },
     { $sort: { Quote_count: -1 } },
     { $limit: 10 }
   ]
)

db.createView(
   "most_retweeted_posts",
   [
     { $unionWith: "Tweets" },
     { $unionWith: "Retweets" },
     { $unionWith: "Quoted_Tweets" },
     { $sort: { Retweet_Count: -1 } },
     { $limit: 10 }
   ]
)
