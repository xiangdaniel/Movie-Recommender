# Movie-Recommender
In this repository, the goal is to build a recommender system that will predict user ratings for films and the datasets are from Netflix Prize data.

In this branch, the Item-based Collabrative Filtering Algorithm is implemented with MapReduce and the recommended movies were sorted and written into database.

The terminal command: 

> hadoop com.sun.tools.javac.Main *.java

> jar cf recommender.jar *.class

> hadoop jar recommender.jar Driver /input /dataDividedByUser /coOccurrenceMatrix /Normalize /Multiplication /Sum 5

| Command | Description |
| --- | --- |
| `#args0: /input` | original dataset directory where you put the Netflix prize data |
| `#args1: /dataDividedByUser` | output directory for DividerByUser job |
| `#args2: /coOccurrenceMatrix` | output directory for coOccurrenceMatrixBuilder job |
| `#args3: /Normalize` | output directory for Normalize job |
| `#args4: /Multiplication` | output directory for Multiplication job |
| `#args5: /Sum` | output directory for Sum job |
| `#args6: 5` | k, the maximum number of recommended movies for each user |
