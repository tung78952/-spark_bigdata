from pyspark import SparkContext

sc = SparkContext(appName="GenderAnalysis_Exercise3")

def parse_rating_line(line):
    """Parse rating data"""
    fields = line.split(",")
    return {
        'user_id': fields[0].strip(),
        'movie_id': fields[1].strip(),
        'rating': float(fields[2].strip()),
        'timestamp': fields[3].strip()
    }

def parse_movie_line(line):
    """Parse movie data"""
    fields = line.split(",")
    return {
        'movie_id': fields[0].strip(),
        'title': fields[1].strip(),
        'genres': fields[2].strip()
    }

def parse_user_line(line):
    """Parse user data"""
    fields = line.split(",")
    return {
        'user_id': fields[0].strip(),
        'gender': fields[1].strip(),
        'age': int(fields[2].strip()),
        'occupation': fields[3].strip(),
        'zipcode': fields[4].strip()
    }

# Load movies
movies_rdd = sc.textFile("./data/movies.txt") \
    .map(parse_movie_line) \
    .map(lambda x: (x['movie_id'], x['title'])) \
    .cache()

# Load users
users_rdd = sc.textFile("./data/users.txt") \
    .map(parse_user_line) \
    .map(lambda x: (x['user_id'], x['gender'])) \
    .cache()

# Load and merge ratings
ratings1 = sc.textFile("./data/ratings_1.txt").map(parse_rating_line)
ratings2 = sc.textFile("./data/ratings_2.txt").map(parse_rating_line)
all_ratings = ratings1.union(ratings2)

# Join ratings with user gender
ratings_with_gender = all_ratings.map(lambda x: (x['user_id'], (x['movie_id'], x['rating']))) \
    .join(users_rdd) \
    .map(lambda x: ((x[1][0][0], x[1][1]), x[1][0][1]))

# Aggregate by (movie_id, gender)
def create_stats(rating):
    return (rating, 1)

def merge_stats(acc, rating):
    return (acc[0] + rating, acc[1] + 1)

def combine_stats(acc1, acc2):
    return (acc1[0] + acc2[0], acc1[1] + acc2[1])

gender_movie_stats = ratings_with_gender.combineByKey(
    create_stats,
    merge_stats,
    combine_stats
)

# Calculate averages
gender_averages = gender_movie_stats.mapValues(lambda x: x[0] / x[1])

# Reorganize data by movie_id
movie_gender_data = gender_averages.map(lambda x: (x[0][0], (x[0][1], x[1]))) \
    .groupByKey() \
    .mapValues(lambda x: dict(x))

# Join with movie titles
final_output = movie_gender_data.join(movies_rdd) \
    .map(lambda x: {
        'movie_id': x[0],
        'title': x[1][1],
        'male_avg': x[1][0].get('M', None),
        'female_avg': x[1][0].get('F', None)
    }) \
    .sortBy(lambda x: x['title'])

# Write to file
with open("./result/bai3.txt", "w", encoding="utf-8") as output_file:
    results = final_output.collect()
    
    for movie in results:
        male_str = f"{movie['male_avg']:.2f}" if movie['male_avg'] is not None else "NA"
        female_str = f"{movie['female_avg']:.2f}" if movie['female_avg'] is not None else "NA"
        
        output_line = f"{movie['title']} - Male_Avg: {male_str}, Female_Avg: {female_str}"
        print(output_line)
        output_file.write(output_line + "\n")

sc.stop()