# Movies-Extract Transform and Load
Learning ETL with Python, Pandas, and  PostgreSQL with Wikipedia and Kaggle Data.

### Contents: 

1. Project Requirements.
2. Assumptions.
3. Summary of Code
4. List of attachments.

### 1 -Project Requirements.  

Create an automated pipeline that takes in new movie data, performs appropriate transformations, and loads data into existing SQL tables. 

The python  script should perform ETL on three movies datasets:

1. Wikipedia movies data: wikipedia.movies.json
2. Kaggle movies metadata: movies_metadata.csv 
3. Kaggle movies ratings: ratings.csv

### 2 -Assumptions.  

1. The client, Amazing Prime, provides a resource that can reliably download fresh data from Wikipedia and Kaggle to the correct folder on the processing server using the given file names.
2. The PostgreSQL server password does not change. 
3. Existing columns in the originating files maintain the same names and types of data.  If columns in data change, then code should be adjusted to handle data in changes. If data types in the columns change then code may need to be adjusted: for example there is a call to keep only wiki columns where nulls are less than 10%, so assume columns of interest will continue to have less than 10% nulls.
4. Wiki fields of  box office, budget, date are not presented in new string pattern forms beyond those forms already identified. The code handles existing string patterns for these fields but would need to be adjusted.
5. New outliers.  The current code handles general patterns detected in analysis of original data. For example, in the merged penultimate dataframe; a box plot of  wiki release_date and kaggle release_date identified an obvious outlier condition which is now handled  in the code. Future data may contain outliers that were not present in analysis of this initial data and therefore not handled by this code.

### 3 -Summary of code

1. Script overview
2. Definition of etl_challenge function which calls the three data sources
3. Imports Dependencies
4. Definition of clean_movie(movie) function used to glean and create alternative titles and change column names.
5. Definition of a dollars parsing function used to parse money fields.
6. Initialization of variables.
7. Read of three data sources. 
8. Wikipedia data cleaning and parsing
   1. filter to films having a named director and imdb link but not being multi-episode
   2. Run the clean_movie function
   3. Extract imdb_id field
   4. Drop duplicates
   5. Keep only columns where sum of nulls is less than 90%.
   6. Transform box office data using string pattern forms.
   7. Transform budget data using string pattern forms.
   8. Transform date field using string pattern forms.
   9. Transform running time field using string pattern.
9. Kaggle movies metadata cleaning and parsing
   1. Drop adult films
   2. Clean video field of bad data
   3. Convert budget, id and popularity to numeric
   4. Convert release_date to datatime
10. Merge the wiki and kaggle movie data into a movies dataframe.
11. Clean and parse of merged movies dataframe
    1. Drop release_date outliers
    2. Convert language list to tuples
    3. Drop unwanted columns, keeping either wiki or kaggle columns    
    4. Create and run function to fill in missing data for runtime, budget and revenue.  Essentially takes data from wiki or kaggle depending on which is better.
    5. Reorder columns
    6. Rename columns
12. Ratings data clean and parsing 
    1. change timestamp to datetime
    2. create rating count fields  using group by movie and rating.
13. Merge ratings count columns with movies dataframe creating movies_with_ratings_df  dataframe
    1. handle nulls
14. Export To SQL
    1. Import password from config.py
    2. Create a connection string  to postgres
    3. Create an engine using SQLAlchemy
    4. Export final movies dataframe to SQL
    5. Import ratings.csv and load to SQL
15. Run the etl_challenge function.

### 4 -List of Attachments

1. Challenge.py: contains the function described above.