from pyspark import SparkContext

sc = SparkContext(appName="MovieRatingAnalysis_Exercise1")

def parse_rating_line(line):
    """Parse rating line and return structured data"""
    fields = line.split(",")
    return {
        'user_id': fields[0].strip(),
        'movie_id': fields[1].strip(),
        'rating': float(fields[2].strip()),
        'timestamp': fields[3].strip()
    }

def parse_movie_line(line):
    """Parse movie line and return structured data"""
    fields = line.split(",")
    return {
        'movie_id': fields[0].strip(),
        'title': fields[1].strip(),
        'genres': fields[2].strip()
    }

# Load movies data
movies_rdd = sc.textFile("./data/movies.txt") \
    .map(parse_movie_line) \
    .map(lambda x: (x['movie_id'], x['title'])) \
    .cache()

# Load ratings from both files
ratings_file1 = sc.textFile("./data/ratings_1.txt").map(parse_rating_line)
ratings_file2 = sc.textFile("./data/ratings_2.txt").map(parse_rating_line)
combined_ratings = ratings_file1.union(ratings_file2)

# Define aggregation functions
def create_combiner(rating):
    return (rating, 1)

def merge_value(accumulator, rating):
    return (accumulator[0] + rating, accumulator[1] + 1)

def merge_combiners(acc1, acc2):
    return (acc1[0] + acc2[0], acc1[1] + acc2[1])

# Calculate movie statistics using aggregateByKey
movie_statistics = combined_ratings.map(lambda x: (x['movie_id'], x['rating'])) \
    .aggregateByKey((0.0, 0), create_combiner, merge_value, merge_combiners)

# Join with movie titles and compute final results
final_results = movie_statistics.join(movies_rdd) \
    .mapValues(lambda x: {
        'title': x[1],
        'sum_ratings': x[0][0],
        'count_ratings': x[0][1],
        'average': x[0][0] / x[0][1]
    }) \
    .sortBy(lambda x: x[1]['title'])

# Write output to file
with open("./result/bai1.txt", "w", encoding="utf-8") as output_file:
    all_results = final_results.collect()
    
    # Print all movies
    for movie_id, stats in all_results:
        output_line = f"{stats['title']} AverageRating: {stats['average']:.2f} (TotalRatings: {stats['count_ratings']})"
        print(output_line)
        output_file.write(output_line + "\n")
    
    # Find and print the best rated movie with at least 5 ratings
    eligible_movies = [(movie_id, stats) for movie_id, stats in all_results 
                       if stats['count_ratings'] >= 5]
    
    if eligible_movies:
        best_movie_id, best_stats = max(eligible_movies, key=lambda x: x[1]['average'])
        summary_line = f"\n{best_stats['title']} is the highest rated movie with an average rating of {best_stats['average']:.2f} among movies with at least 5 ratings."
        print(summary_line)
        output_file.write(summary_line + "\n")

sc.stop()