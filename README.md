# Movie-Recommender
In this repository, the goal is to build a recommender system that will predict user ratings for films and the datasets are from Netflix Prize data.

In this predict-ratings branch, the Item-based Collabrative Filtering Algorithm is implemented with MapReduce.

> hadoop com.sun.tools.javac.Main *.java

> jar cf recommender.jar *.class

> hadoop jar recommender.jar Driver /input /dataDividedByUser /coOccurrenceMatrix /Normalize /Multiplication /Sum

| Command | Description |
| --- | --- |
| `#args0: /input` | original dataset directory where you put the Netflix prize data |
| `#args1: /dataDividedByUser` | output directory for DividerByUser job |
| `#args2: /coOccurrenceMatrix` | output directory for coOccurrenceMatrixBuilder job |
| `#args3: /Normalize` | output directory for Normalize job |
| `#args4: /Multiplication` | output directory for Multiplication job |
| `#args5: /Sum` | output directory for Sum job |
