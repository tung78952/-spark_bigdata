# Báo Cáo Lab Spark - Phân Tích Dữ Liệu Phim

## Thông Tin

- **Môn học**: IE212 - Big Data
- **Họ tên sinh viên**: Trần Phan Thanh Tùng
- **MSSV**: 23521747

---

## Dựng Spark Cluster với Docker

### 1. Cấu trúc Docker Compose

Hệ thống sử dụng Docker để triển khai Apache Spark cluster với các thành phần:

- **Spark Master Node**: Quản lý và điều phối toàn bộ cluster
- **Spark Worker Node**: Xử lý và thực thi các tác vụ được phân phối

### 2. File `docker-compose.yml`

```yaml
services:
  spark-master:
    image: apache/spark
    container_name: spark-master
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
    ports:
      - "8080:8080"
      - "7077:7077"
    volumes:
      - .:/opt/spark/work-dir

  spark-worker:
    image: apache/spark
    container_name: spark-worker
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077
    depends_on:
      - spark-master
    volumes:
      - .:/opt/spark/work-dir
```

### 3. Khởi động và Quản lý Spark Cluster

```bash
# Khởi động cluster
docker-compose up -d

# Kiểm tra trạng thái các container
docker-compose ps

# Xem logs
docker-compose logs

# Dừng cluster
docker-compose down
```

### 4. Truy cập Spark Web UI

- Master UI: http://localhost:8080
- Master URL: spark://spark-master:7077

---

## Cấu Trúc Thư Mục Project

```
spark_bigdata-main/
├── data/                      # Thư mục chứa dữ liệu đầu vào
│   ├── movies.txt            # Thông tin phim
│   ├── ratings_1.txt         # Dữ liệu đánh giá phần 1
│   ├── ratings_2.txt         # Dữ liệu đánh giá phần 2
│   ├── users.txt             # Thông tin người dùng
│   └── occupation.txt        # Danh mục nghề nghiệp
├── result/                    # Thư mục chứa kết quả xử lý
│   ├── bai1.txt
│   ├── bai2.txt
│   ├── bai3.txt
│   ├── bai4.txt
│   ├── bai5.txt
│   └── bai6.txt
├── bai1.py                   # Script phân tích đánh giá trung bình
├── bai2.py                   # Script phân tích theo thể loại
├── bai3.py                   # Script phân tích theo giới tính
├── bai4.py                   # Script phân tích theo nhóm tuổi
├── bai5.py                   # Script phân tích theo nghề nghiệp
├── bai6.py                   # Script phân tích theo thời gian
└── docker-compose.yml        # Cấu hình Docker Compose
```

---

## Mô Tả Dữ Liệu

### 1. movies.txt

- **Schema**: MovieID, Title, Genres
- **Mô tả**: Chứa thông tin về phim, thể loại (phân tách bằng "|")

### 2. ratings_1.txt & ratings_2.txt

- **Schema**: UserID, MovieID, Rating, Timestamp
- **Mô tả**: Dữ liệu đánh giá của người dùng cho các phim

### 3. users.txt

- **Schema**: UserID, Gender, Age, Occupation, Zip-code
- **Mô tả**: Thông tin chi tiết về người dùng

### 4. occupation.txt

- **Schema**: ID, Occupation
- **Mô tả**: Bảng tra cứu nghề nghiệp

---

## Hướng Dẫn Chạy Các Bài Tập

### Phương pháp 1: Sử dụng Docker (Khuyến nghị)

```bash
# Bài 1: Tính điểm đánh giá trung bình và tổng số lượt đánh giá
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/bai1.py

# Bài 2: Phân tích đánh giá theo thể loại phim
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/bai2.py

# Bài 3: Phân tích đánh giá theo giới tính người dùng
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/bai3.py

# Bài 4: Phân tích đánh giá theo nhóm tuổi
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/bai4.py

# Bài 5: Phân tích đánh giá theo nghề nghiệp
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/bai5.py

# Bài 6: Phân tích đánh giá theo năm
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/bai6.py
```

### Phương pháp 2: Chạy trực tiếp (Nếu đã cài PySpark local)

```bash
python bai1.py
python bai2.py
python bai3.py
python bai4.py
python bai5.py
python bai6.py
```

---

## Chi Tiết Kỹ Thuật Từng Bài

### Bài 1: Tính Điểm Đánh Giá Trung Bình và Tổng Số Lượt Đánh Giá

**Mục tiêu:**

- Gộp dữ liệu từ 2 file ratings
- Tính điểm trung bình cho mỗi phim
- Tìm phim có rating cao nhất (tối thiểu 5 lượt đánh giá)

**Kỹ thuật sử dụng:**

- `aggregateByKey()`: Tổng hợp dữ liệu theo key với hiệu suất cao
- `join()`: Kết hợp dữ liệu phim với ratings
- Xử lý dữ liệu dạng dictionary để dễ đọc

**Kết quả:**

```
E.T. the Extra-Terrestrial (1982) AverageRating: 3.67 (TotalRatings: 18)
Fight Club (1999) AverageRating: 3.50 (TotalRatings: 7)
Gladiator (2000) AverageRating: 3.61 (TotalRatings: 18)
Lawrence of Arabia (1962) AverageRating: 3.44 (TotalRatings: 18)
Mad Max: Fury Road (2015) AverageRating: 3.47 (TotalRatings: 18)
No Country for Old Men (2007) AverageRating: 3.89 (TotalRatings: 18)
Psycho (1960) AverageRating: 4.00 (TotalRatings: 2)
Sunset Boulevard (1950) AverageRating: 4.36 (TotalRatings: 7)
The Godfather: Part II (1974) AverageRating: 4.00 (TotalRatings: 17)
The Lord of the Rings: The Fellowship of the Ring (2001) AverageRating: 3.89 (TotalRatings: 18)
The Lord of the Rings: The Return of the King (2003) AverageRating: 3.82 (TotalRatings: 11)
The Silence of the Lambs (1991) AverageRating: 3.14 (TotalRatings: 7)
The Social Network (2010) AverageRating: 3.86 (TotalRatings: 7)
The Terminator (1984) AverageRating: 4.06 (TotalRatings: 18)

Sunset Boulevard (1950) is the highest rated movie with an average rating of 4.36 among movies with at least 5 ratings.
```

---

### Bài 2: Phân Tích Đánh Giá Theo Thể Loại

**Mục tiêu:**

- Tách các thể loại từ chuỗi phân tách bằng "|"
- Tính rating trung bình cho từng thể loại

**Kỹ thuật sử dụng:**

- `flatMap()`: Tách một phim thành nhiều genre
- `combineByKey()`: Tổng hợp thống kê theo genre
- `join()`: Kết hợp genre với ratings

**Kết quả:**

```
Action - AverageRating: 3.71 (TotalRatings: 54)
Adventure - AverageRating: 3.63 (TotalRatings: 83)
Biography - AverageRating: 3.56 (TotalRatings: 25)
Crime - AverageRating: 3.81 (TotalRatings: 42)
Drama - AverageRating: 3.76 (TotalRatings: 128)
Family - AverageRating: 3.67 (TotalRatings: 18)
Fantasy - AverageRating: 3.86 (TotalRatings: 29)
Film-Noir - AverageRating: 4.36 (TotalRatings: 7)
Horror - AverageRating: 4.00 (TotalRatings: 2)
Mystery - AverageRating: 4.00 (TotalRatings: 2)
Sci-Fi - AverageRating: 3.73 (TotalRatings: 54)
Thriller - AverageRating: 3.70 (TotalRatings: 27)
```

---

### Bài 3: Phân Tích Đánh Giá Theo Giới Tính

**Mục tiêu:**

- Join ratings với user data để lấy thông tin giới tính
- Tính rating trung bình riêng cho nam và nữ

**Kỹ thuật sử dụng:**

- Multiple `join()`: Kết nối ratings, users, movies
- `groupByKey()`: Nhóm dữ liệu theo phim
- Xử lý dữ liệu thiếu (NA) khi không có rating từ một giới tính

**Kết quả:**

```
E.T. the Extra-Terrestrial (1982) - Male_Avg: 3.81, Female_Avg: 3.55
Fight Club (1999) - Male_Avg: 3.50, Female_Avg: 3.50
Gladiator (2000) - Male_Avg: 3.59, Female_Avg: 3.64
Lawrence of Arabia (1962) - Male_Avg: 3.55, Female_Avg: 3.31
Mad Max: Fury Road (2015) - Male_Avg: 4.00, Female_Avg: 3.32
No Country for Old Men (2007) - Male_Avg: 3.92, Female_Avg: 3.83
Psycho (1960) - Male_Avg: NA, Female_Avg: 4.00
Sunset Boulevard (1950) - Male_Avg: 4.33, Female_Avg: 4.50
The Godfather: Part II (1974) - Male_Avg: 4.06, Female_Avg: 3.94
The Lord of the Rings: The Fellowship of the Ring (2001) - Male_Avg: 4.00, Female_Avg: 3.80
The Lord of the Rings: The Return of the King (2003) - Male_Avg: 3.75, Female_Avg: 3.90
The Silence of the Lambs (1991) - Male_Avg: 3.33, Female_Avg: 3.00
The Social Network (2010) - Male_Avg: 4.00, Female_Avg: 3.67
The Terminator (1984) - Male_Avg: 3.93, Female_Avg: 4.14
```

---

### Bài 4: Phân Tích Đánh Giá Theo Nhóm Tuổi

**Mục tiêu:**

- Phân loại users thành các nhóm tuổi: 0-18, 18-35, 35-50, 50+
- Tính rating trung bình cho mỗi nhóm tuổi với từng phim

**Kỹ thuật sử dụng:**

- Function `categorize_age()`: Phân loại tuổi
- `combineByKey()`: Tổng hợp theo (movie_id, age_group)
- Dictionary comprehension: Format output theo các nhóm tuổi chuẩn

**Kết quả:**

```
E.T. the Extra-Terrestrial (1982) - [0-18: NA, 18-35: 3.56, 35-50: 3.83, 50+: 3.00]
Fight Club (1999) - [0-18: NA, 18-35: 3.50, 35-50: 3.50, 50+: 3.50]
Gladiator (2000) - [0-18: NA, 18-35: 3.44, 35-50: 3.81, 50+: 3.50]
Lawrence of Arabia (1962) - [0-18: NA, 18-35: 3.60, 35-50: 3.29, 50+: 4.50]
Mad Max: Fury Road (2015) - [0-18: NA, 18-35: 3.36, 35-50: 3.64, 50+: NA]
No Country for Old Men (2007) - [0-18: NA, 18-35: 3.81, 35-50: 3.94, 50+: 4.00]
Psycho (1960) - [0-18: NA, 18-35: 4.50, 35-50: 3.50, 50+: NA]
Sunset Boulevard (1950) - [0-18: NA, 18-35: 4.17, 35-50: 4.50, 50+: NA]
The Godfather: Part II (1974) - [0-18: NA, 18-35: 3.78, 35-50: 4.25, 50+: NA]
The Lord of the Rings: The Fellowship of the Ring (2001) - [0-18: NA, 18-35: 4.00, 35-50: 3.83, 50+: NA]
The Lord of the Rings: The Return of the King (2003) - [0-18: NA, 18-35: 3.83, 35-50: 3.81, 50+: NA]
The Silence of the Lambs (1991) - [0-18: NA, 18-35: 3.00, 35-50: 3.25, 50+: NA]
The Social Network (2010) - [0-18: NA, 18-35: 4.00, 35-50: 3.67, 50+: NA]
The Terminator (1984) - [0-18: NA, 18-35: 4.17, 35-50: 4.05, 50+: 3.75]
```

---

### Bài 5: Phân Tích Đánh Giá Theo Nghề Nghiệp

**Mục tiêu:**

- Join users với occupation table
- Tính rating trung bình theo từng nghề nghiệp

**Kỹ thuật sử dụng:**

- Chain joins: users -> occupations -> ratings
- `combineByKey()`: Tổng hợp statistics theo occupation
- `cache()`: Tối ưu hóa cho RDD được sử dụng nhiều lần

**Kết quả:**

```
Accountant - AverageRating: 3.58 (TotalRatings: 6)
Artist - AverageRating: 3.73 (TotalRatings: 11)
Consultant - AverageRating: 3.86 (TotalRatings: 14)
Designer - AverageRating: 4.00 (TotalRatings: 13)
Doctor - AverageRating: 3.69 (TotalRatings: 21)
Engineer - AverageRating: 3.56 (TotalRatings: 18)
Journalist - AverageRating: 3.85 (TotalRatings: 17)
Lawyer - AverageRating: 3.65 (TotalRatings: 17)
Manager - AverageRating: 3.47 (TotalRatings: 16)
Nurse - AverageRating: 3.86 (TotalRatings: 11)
Programmer - AverageRating: 4.25 (TotalRatings: 10)
Salesperson - AverageRating: 3.65 (TotalRatings: 17)
Student - AverageRating: 4.00 (TotalRatings: 8)
Teacher - AverageRating: 3.70 (TotalRatings: 5)
```

---

### Bài 6: Phân Tích Đánh Giá Theo Thời Gian

**Mục tiêu:**

- Chuyển đổi Unix timestamp thành năm
- Tính tổng ratings và rating trung bình theo từng năm

**Kỹ thuật sử dụng:**

- `datetime` module: Chuyển đổi timestamp
- `combineByKey()`: Tổng hợp theo năm
- Time-series analysis

**Kết quả:**

```
2020 - TotalRatings: 184, AverageRating: 3.75
```

---

## Kỹ Thuật RDD Được Sử Dụng

### 1. Transformations

- `map()`: Biến đổi từng phần tử
- `flatMap()`: Biến đổi và flatten kết quả
- `filter()`: Lọc dữ liệu
- `join()`: Kết hợp RDD theo key
- `groupByKey()`: Nhóm values theo key
- `sortBy()`: Sắp xếp dữ liệu
- `union()`: Gộp nhiều RDD

### 2. Actions

- `collect()`: Thu thập tất cả dữ liệu về driver
- `count()`: Đếm số phần tử
- `take()`: Lấy n phần tử đầu

### 3. Advanced Operations

- `aggregateByKey()`: Tổng hợp với accumulator tùy chỉnh
- `combineByKey()`: Tổng hợp linh hoạt với 3 hàm
- `mapValues()`: Transform chỉ values, giữ nguyên keys
- `cache()`/`persist()`: Lưu RDD trong memory

---

## Tối Ưu Hóa Được Áp Dụng

1. **Caching**: Cache các RDD được sử dụng nhiều lần (movies, users)
2. **Efficient Aggregation**: Sử dụng `combineByKey()` thay vì `reduceByKey()` cho aggregation phức tạp
3. **Data Structure**: Sử dụng dictionary thay vì tuple để code rõ ràng hơn
4. **Minimize Shuffling**: Sắp xếp operations để giảm data shuffling
5. **Lazy Evaluation**: Tận dụng lazy evaluation của Spark để tối ưu execution plan

---

## Kết Luận

Project này đã thành công trong việc:

- ✅ Triển khai Spark cluster với Docker
- ✅ Xử lý và phân tích dữ liệu phim quy mô lớn
- ✅ Áp dụng các RDD operations hiệu quả
- ✅ Thực hiện join và aggregation phức tạp
- ✅ Tối ưu hóa hiệu suất với caching và partitioning

**Insight quan trọng:**

- Film-Noir là thể loại có rating cao nhất (4.36)
- Programmer có xu hướng đánh giá cao nhất (4.25)
- Phim "Sunset Boulevard (1950)" được đánh giá cao nhất với tối thiểu 5 ratings
- Xu hướng rating khá ổn định qua các nhóm tuổi và giới tính

---

## Tài Liệu Tham Khảo

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [Docker Documentation](https://docs.docker.com/)
- [RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
