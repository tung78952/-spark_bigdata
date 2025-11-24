from pyspark import SparkContext
from datetime import datetime

sc = SparkContext(appName="TimeAnalysis_Exercise6")

def parse_rating_line(line):
    """Parse rating entry"""
    fields = line.split(",")
    return {
        'user_id': fields[0].strip(),
        'movie_id': fields[1].strip(),
        'rating': float(fields[2].strip()),
        'timestamp': int(fields[3].strip())
    }

def extract_year_from_timestamp(timestamp):
    """Convert Unix timestamp to year"""
    return datetime.fromtimestamp(timestamp).year

# Load ratings from both files
ratings_source1 = sc.textFile("./data/ratings_1.txt").map(parse_rating_line)
ratings_source2 = sc.textFile("./data/ratings_2.txt").map(parse_rating_line)
merged_ratings = ratings_source1.union(ratings_source2)

# Extract year and rating
year_rating_pairs = merged_ratings.map(lambda x: (
    extract_year_from_timestamp(x['timestamp']), 
    x['rating']
))

# Aggregate by year
def initialize_year_stats(rating):
    return (rating, 1)

def update_year_stats(acc, rating):
    return (acc[0] + rating, acc[1] + 1)

def combine_year_stats(acc1, acc2):
    return (acc1[0] + acc2[0], acc1[1] + acc2[1])

yearly_statistics = year_rating_pairs.combineByKey(
    initialize_year_stats,
    update_year_stats,
    combine_year_stats
)

# Calculate yearly averages
yearly_results = yearly_statistics.mapValues(lambda x: {
    'total_ratings': x[1],
    'sum_ratings': x[0],
    'average_rating': x[0] / x[1]
}).sortByKey()

# Write results to file
with open("./result/bai6.txt", "w", encoding="utf-8") as output_file:
    year_data = yearly_results.collect()
    
    for year, stats in year_data:
        output_line = f"{year} - TotalRatings: {stats['total_ratings']}, AverageRating: {stats['average_rating']:.2f}"
        print(output_line)
        output_file.write(output_line + "\n")

sc.stop()