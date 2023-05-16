# Movie Recommender
A backend system that recommends movies to users based on Alternating Least Squares.

It is implemented in Scala by Manos Chatzakis, at EPFL (emmanouil.chatzakis@epfl.ch)

## Functionalities
* Data Loading: The system parses files of movies and ratings
* Analytics: Provides basic analytics about the provided datasets
* Aggregator: Provides aggregator functionalities about the movies and ratings. 
It also supports online data updates for the ratings
* Recommender: Baseline and ALS-based recommendations

## Movie Recommendations
Given a user and the preferred genres, the system predicts the ratings of the user for movies 
that match the genres using Alternating Least Squares, and returns the top predictions to
the user.

## Online Rating Updates
The system supports online rating updates. It calculates the updated rating of a 
movie using the formula
```math
updatedRating = \frac{newRatingsSum + oldRatingsSum}{oldUsers+newUsers}
```
which is
an optimized approach to calculate online ratings. 

## Spark Partitions
The system is designed for Spark. The default number for partitions is 8.

## About
The system was implemented as part of the 
course "cs460: Systems for Data Management and Data science" of EPFL, during autumn semester 
of 2023.

