# Movie-Recommender
In this repository, the goal is to build a recommender system that will predict user ratings for films and the datasets are from Netflix Prize data.

In this branch, the Item-based Collabrative Filtering Algorithm is implemented with MapReduce and the recommended movies were sorted and written into database.

The terminal command: 

hadoop com.sun.tools.javac.Main *.java
jar cf recommender.jar *.class
hadoop jar recommender.jar Driver /input /dataDividedByUser /coOccurrenceMatrix /Normalize /Multiplication /Sum 5


#args0: original dataset directory is /input where you put the Netflix prize data

#args1: output directory for DividerByUser job

#args2: output directory for coOccurrenceMatrixBuilder job

#args3: output directory for Normalize job

#args4: output directory for Multiplication job

#args5: output directory for Sum job

#args6: k, the maximum number of recommended movies in database
