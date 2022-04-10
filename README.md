# Automate ETL (Spark Scala)
## _Add Surrogate Key to a DataFrame_


[![Build Status](https://travis-ci.org/joemccann/dillinger.svg?branch=master)](https://travis-ci.org/joemccann/dillinger)

```scala
def genSurrogateKey(
  df: DataFrame, //the DataFrame that contains all cols
  newPKCol: String, //the new col as Surrogate Key
  baseOnCols: ColList // list of cols to generate the Key based on
): DataFrame
)
```

> The Result will apply to a new DataFrame.