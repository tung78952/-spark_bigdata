from pyspark import SparkContext

sc = SparkContext(appName="AgeGroupAnalysis_Exercise4")

def parse_rating_line(line):
    """Parse rating information"""
    fields = line.split(",")
    return {
        'user_id': fields[0].strip(),
        'movie_id': fields[1].strip(),
        'rating': float(fields[2].strip()),
        'timestamp': fields[3].strip()
    }

def parse_movie_line(line):
    """Parse movie information"""
    fields = line.split(",")
    return {
        'movie_id': fields[0].strip(),
        'title': fields[1].strip(),
        'genres': fields[2].strip()
    }

def parse_user_line(line):
    """Parse user information"""
    fields = line.split(",")
    return {
        'user_id': fields[0].strip(),
        'gender': fields[1].strip(),
        'age': int(fields[2].strip()),
        'occupation': fields[3].strip(),
        'zipcode': fields[4].strip()
    }

def categorize_age(age):
    """Categorize age into groups"""
    if age < 18:
        return "0-18"
    elif age < 35:
        return "18-35"
    elif age < 50:
        return "35-50"
    else:
        return "50+"

# Load movies data
movies_rdd = sc.textFile("./data/movies.txt") \
    .map(parse_movie_line) \
    .map(lambda x: (x['movie_id'], x['title'])) \
    .cache()

# Load users and categorize by age
users_rdd = sc.textFile("./data/users.txt") \
    .map(parse_user_line) \
    .map(lambda x: (x['user_id'], categorize_age(x['age']))) \
    .cache()

# Load and combine ratings
ratings_set1 = sc.textFile("./data/ratings_1.txt").map(parse_rating_line)
ratings_set2 = sc.textFile("./data/ratings_2.txt").map(parse_rating_line)
combined_ratings = ratings_set1.union(ratings_set2)

# Join ratings with age groups
ratings_with_age = combined_ratings.map(lambda x: (x['user_id'], (x['movie_id'], x['rating']))) \
    .join(users_rdd) \
    .map(lambda x: ((x[1][0][0], x[1][1]), x[1][0][1]))

# Compute statistics by (movie_id, age_group)
def init_stats(rating):
    return (rating, 1)

def update_stats(acc, rating):
    return (acc[0] + rating, acc[1] + 1)

def merge_stats(acc1, acc2):
    return (acc1[0] + acc2[0], acc1[1] + acc2[1])

age_movie_statistics = ratings_with_age.combineByKey(
    init_stats,
    update_stats,
    merge_stats
)

# Calculate average ratings
age_group_averages = age_movie_statistics.mapValues(lambda x: x[0] / x[1])

# Group by movie_id
movie_age_groups = age_group_averages.map(lambda x: (x[0][0], (x[0][1], x[1]))) \
    .groupByKey() \
    .mapValues(lambda x: dict(x))

# Join with movie titles
results_with_titles = movie_age_groups.join(movies_rdd)

# Format the output
age_categories = ["0-18", "18-35", "35-50", "50+"]

formatted_results = results_with_titles.map(lambda x: {
    'movie_id': x[0],
    'title': x[1][1],
    'age_groups': x[1][0]
}).sortBy(lambda x: x['title'])

# Write output to file
with open("./result/bai4.txt", "w", encoding="utf-8") as output_file:
    all_results = formatted_results.collect()
    
    for movie in all_results:
        age_stats = []
        for age_cat in age_categories:
            if age_cat in movie['age_groups']:
                avg_val = f"{movie['age_groups'][age_cat]:.2f}"
            else:
                avg_val = "NA"
            age_stats.append(f"{age_cat}: {avg_val}")
        
        output_line = f"{movie['title']} - [{', '.join(age_stats)}]"
        print(output_line)
        output_file.write(output_line + "\n")

sc.stop()