from pyspark import SparkContext

sc = SparkContext(appName="GenreAnalysis_Exercise2")

def parse_rating_line(line):
    """Parse rating line"""
    fields = line.split(",")
    return {
        'user_id': fields[0].strip(),
        'movie_id': fields[1].strip(),
        'rating': float(fields[2].strip()),
        'timestamp': fields[3].strip()
    }

def parse_movie_line(line):
    """Parse movie line"""
    fields = line.split(",")
    return {
        'movie_id': fields[0].strip(),
        'title': fields[1].strip(),
        'genres': fields[2].strip()
    }

def extract_genres(movie_data):
    """Extract individual genres from a movie"""
    movie_id = movie_data['movie_id']
    genre_list = movie_data['genres'].split("|")
    return [(genre.strip(), movie_id) for genre in genre_list]

# Load and parse movies
movies_rdd = sc.textFile("./data/movies.txt") \
    .map(parse_movie_line) \
    .cache()

# Create genre to movie_id mapping
genre_to_movie = movies_rdd.flatMap(extract_genres)

# Load and combine ratings
ratings_part1 = sc.textFile("./data/ratings_1.txt").map(parse_rating_line)
ratings_part2 = sc.textFile("./data/ratings_2.txt").map(parse_rating_line)
all_ratings = ratings_part1.union(ratings_part2)

# Create movie_id to rating mapping
movie_to_rating = all_ratings.map(lambda x: (x['movie_id'], x['rating']))

# Join genre with ratings
genre_ratings = genre_to_movie.join(movie_to_rating) \
    .map(lambda x: (x[1][0], x[1][1]))

# Aggregate by genre using combineByKey
def initialize_accumulator(rating):
    return (rating, 1)

def add_to_accumulator(acc, rating):
    return (acc[0] + rating, acc[1] + 1)

def combine_accumulators(acc1, acc2):
    return (acc1[0] + acc2[0], acc1[1] + acc2[1])

genre_statistics = genre_ratings.combineByKey(
    initialize_accumulator,
    add_to_accumulator,
    combine_accumulators
)

# Calculate averages and format output
genre_results = genre_statistics.mapValues(lambda x: {
    'total_score': x[0],
    'num_ratings': x[1],
    'avg_rating': x[0] / x[1]
}).sortByKey()

# Write results to file
with open("./result/bai2.txt", "w", encoding="utf-8") as output_file:
    collected_genres = genre_results.collect()
    
    for genre, stats in collected_genres:
        output_line = f"{genre} - AverageRating: {stats['avg_rating']:.2f} (TotalRatings: {stats['num_ratings']})"
        print(output_line)
        output_file.write(output_line + "\n")

sc.stop()