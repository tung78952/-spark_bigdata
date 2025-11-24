from pyspark import SparkContext

sc = SparkContext(appName="OccupationAnalysis_Exercise5")

def parse_rating_line(line):
    """Parse rating record"""
    fields = line.split(",")
    return {
        'user_id': fields[0].strip(),
        'movie_id': fields[1].strip(),
        'rating': float(fields[2].strip()),
        'timestamp': fields[3].strip()
    }

def parse_user_line(line):
    """Parse user record"""
    fields = line.split(",")
    return {
        'user_id': fields[0].strip(),
        'gender': fields[1].strip(),
        'age': int(fields[2].strip()),
        'occupation_id': fields[3].strip(),
        'zipcode': fields[4].strip()
    }

def parse_occupation_line(line):
    """Parse occupation record"""
    fields = line.split(",")
    return {
        'occupation_id': fields[0].strip(),
        'occupation_name': fields[1].strip()
    }

# Load occupation mapping
occupations_rdd = sc.textFile("./data/occupation.txt") \
    .map(parse_occupation_line) \
    .map(lambda x: (x['occupation_id'], x['occupation_name'])) \
    .cache()

# Load users with occupation
users_rdd = sc.textFile("./data/users.txt") \
    .map(parse_user_line) \
    .map(lambda x: (x['occupation_id'], x['user_id']))

# Join users with occupations
user_occupation_map = users_rdd.join(occupations_rdd) \
    .map(lambda x: (x[1][0], x[1][1])) \
    .cache()

# Load and merge ratings
ratings_batch1 = sc.textFile("./data/ratings_1.txt").map(parse_rating_line)
ratings_batch2 = sc.textFile("./data/ratings_2.txt").map(parse_rating_line)
all_ratings_data = ratings_batch1.union(ratings_batch2)

# Join ratings with occupations
ratings_by_occupation = all_ratings_data.map(lambda x: (x['user_id'], x['rating'])) \
    .join(user_occupation_map) \
    .map(lambda x: (x[1][1], x[1][0]))

# Aggregate statistics by occupation
def start_accumulator(rating):
    return (rating, 1)

def add_to_stats(accumulator, rating):
    return (accumulator[0] + rating, accumulator[1] + 1)

def merge_accumulators(acc1, acc2):
    return (acc1[0] + acc2[0], acc1[1] + acc2[1])

occupation_stats = ratings_by_occupation.combineByKey(
    start_accumulator,
    add_to_stats,
    merge_accumulators
)

# Calculate final statistics
occupation_results = occupation_stats.mapValues(lambda x: {
    'sum_ratings': x[0],
    'count_ratings': x[1],
    'average_rating': x[0] / x[1]
}).sortByKey()

# Write output
with open("./result/bai5.txt", "w", encoding="utf-8") as output_file:
    collected_results = occupation_results.collect()
    
    for occupation, stats in collected_results:
        output_line = f"{occupation} - AverageRating: {stats['average_rating']:.2f} (TotalRatings: {stats['count_ratings']})"
        print(output_line)
        output_file.write(output_line + "\n")

sc.stop()