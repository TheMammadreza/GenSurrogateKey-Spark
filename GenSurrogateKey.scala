import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

type ColList = Seq[String]

def existsCol(df: DataFrame, colName: String, caseSensitive: Boolean = false, ignoreSpace: Boolean = true): Boolean = {
  
  def fixStr(str: String): String = {
    var result: String = str.trim()
    
    if (caseSensitive)
      result = result.toUpperCase()
    if (ignoreSpace)
      result = result.replaceAll(" ", "")
    
    return result
  } //def
  
  return df.columns.map(fixStr(_)).contains(fixStr(colName))
} //def

def replaceAllValues(colList: ColList, oldValue: Any, newValue: Any): Array[Column] = {
    return colList.toArray.map(x => { when(col(x) === oldValue, newValue).otherwise(col(x)).alias(x) })
} //def

def genSurrogateKey(df: DataFrame, newPKCol: String, baseOnCols: ColList): DataFrame = {
  val nullStr: String = "!__(NULL)__!"
  val arrangedCols: ColList = df.schema.fieldNames.toSeq++Seq(newPKCol)
  var result: DataFrame = df.na.fill(nullStr)
  
  result = result.select(baseOnCols.map(x => col(x)): _*)
    .distinct()
    .selectExpr(s"UUID() AS $newPKCol", "*")
    .join(result, baseOnCols, "right")
    .select(arrangedCols.map(x => col(x)): _*)
    .select(replaceAllValues(arrangedCols, nullStr, null): _*)

  return result
} //def