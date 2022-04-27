# Pyspark Helper Functions
### [For less verbose and foolproof operations]


```python
try:
    from pyspark import SparkConf
except ImportError:
    ! pip install pyspark==3.2.1

from pyspark import SparkConf
from pyspark.sql import SparkSession, types as st
from IPython.display import HTML

import src.spark.helpers as sh
```


```python
# Setup Spark

conf = SparkConf().setMaster("local[1]").setAppName("examples")
spark = SparkSession.builder.config(conf=conf).getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
```


```python
# Load example datasets

dataframe_1 = spark.read.options(header=True).csv("./data/dataset_1.csv")
dataframe_2 = spark.read.options(header=True).csv("./data/dataset_2.csv")
html = (
    "<div style='float:left'><h4>Dataset 1:</h3>" +
    dataframe_1.toPandas().to_html() + 
    "</div><div style='float:left; margin-left:50px;'><h4>Dataset 2:</h3>" +
    dataframe_2.toPandas().to_html() +
    "</div>"
)
HTML(html)
```




<div style='float:left'><h4>Dataset 1:</h3><table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>x1</th>
      <th>x2</th>
      <th>x3</th>
      <th>x4</th>
      <th>x5</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>J</td>
      <td>734</td>
      <td>499</td>
      <td>595.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B</td>
      <td>J</td>
      <td>357</td>
      <td>202</td>
      <td>525.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>C</td>
      <td>H</td>
      <td>864</td>
      <td>568</td>
      <td>433.5</td>
    </tr>
    <tr>
      <th>3</th>
      <td>D</td>
      <td>J</td>
      <td>530</td>
      <td>703</td>
      <td>112.3</td>
    </tr>
    <tr>
      <th>4</th>
      <td>E</td>
      <td>H</td>
      <td>61</td>
      <td>521</td>
      <td>906.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>F</td>
      <td>H</td>
      <td>482</td>
      <td>496</td>
      <td>13.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>G</td>
      <td>A</td>
      <td>350</td>
      <td>279</td>
      <td>941.0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>H</td>
      <td>C</td>
      <td>171</td>
      <td>267</td>
      <td>423.0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>I</td>
      <td>C</td>
      <td>755</td>
      <td>133</td>
      <td>600.0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>J</td>
      <td>A</td>
      <td>228</td>
      <td>765</td>
      <td>7.0</td>
    </tr>
  </tbody>
</table></div><div style='float:left; margin-left:50px;'><h4>Dataset 2:</h3><table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>x1</th>
      <th>x3</th>
      <th>x4</th>
      <th>x6</th>
      <th>x7</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>W</td>
      <td>K</td>
      <td>391</td>
      <td>140</td>
      <td>872.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>X</td>
      <td>G</td>
      <td>88</td>
      <td>483</td>
      <td>707.1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Y</td>
      <td>M</td>
      <td>144</td>
      <td>476</td>
      <td>714.3</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Z</td>
      <td>J</td>
      <td>896</td>
      <td>68</td>
      <td>902.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>A</td>
      <td>O</td>
      <td>946</td>
      <td>187</td>
      <td>431.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>B</td>
      <td>P</td>
      <td>692</td>
      <td>523</td>
      <td>503.5</td>
    </tr>
    <tr>
      <th>6</th>
      <td>C</td>
      <td>Q</td>
      <td>550</td>
      <td>988</td>
      <td>181.05</td>
    </tr>
    <tr>
      <th>7</th>
      <td>D</td>
      <td>R</td>
      <td>50</td>
      <td>419</td>
      <td>42.0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>E</td>
      <td>S</td>
      <td>824</td>
      <td>805</td>
      <td>558.2</td>
    </tr>
    <tr>
      <th>9</th>
      <td>F</td>
      <td>T</td>
      <td>69</td>
      <td>722</td>
      <td>721.0</td>
    </tr>
  </tbody>
</table></div>



## 1. Pandas-like group by


```python
for group, data in sh.group_iterator(dataframe_1, "x2"):
    print(group, " => ", data.toPandas().shape[0])
```

    A  =>  2
    C  =>  2
    H  =>  3
    J  =>  3


### [Multiple columns group by]


```python
for group, data in sh.group_iterator(dataframe_1, ["x1", "x2"]):
    print(group, " => ", data.toPandas().shape[0])
```

    ('A', 'J')  =>  1
    ('B', 'J')  =>  1
    ('C', 'H')  =>  1
    ('D', 'J')  =>  1
    ('E', 'H')  =>  1
    ('F', 'H')  =>  1
    ('G', 'A')  =>  1
    ('H', 'C')  =>  1
    ('I', 'C')  =>  1
    ('J', 'A')  =>  1


## 2. Bulk-change schema


```python
before = [(x["name"], x["type"]) for x in dataframe_1.schema.jsonValue()["fields"]]

schema = {
    "x2": st.IntegerType(),
    "x5": st.FloatType(),
}
new_dataframe = sh.change_schema(dataframe_1, schema)

after = [(x["name"], x["type"]) for x in new_dataframe.schema.jsonValue()["fields"]]
check = [
    ('x1', 'string'),
    ('x2', 'integer'),
    ('x3', 'string'),
    ('x4', 'string'),
    ('x5', 'float')
]

assert before != after
assert after == check
```

## 3. Improved joins


```python
joined = sh.join(dataframe_1.select("x2", "x5"), dataframe_2, sh.JoinStatement("x2", "x1"))
joined.toPandas()
```




<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>x1</th>
      <th>x2</th>
      <th>x3</th>
      <th>x4</th>
      <th>x5</th>
      <th>x6</th>
      <th>x7</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>A</td>
      <td>O</td>
      <td>946</td>
      <td>7.0</td>
      <td>187</td>
      <td>431.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A</td>
      <td>A</td>
      <td>O</td>
      <td>946</td>
      <td>941.0</td>
      <td>187</td>
      <td>431.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>C</td>
      <td>C</td>
      <td>Q</td>
      <td>550</td>
      <td>600.0</td>
      <td>988</td>
      <td>181.05</td>
    </tr>
    <tr>
      <th>3</th>
      <td>C</td>
      <td>C</td>
      <td>Q</td>
      <td>550</td>
      <td>423.0</td>
      <td>988</td>
      <td>181.05</td>
    </tr>
  </tbody>
</table>
</div>



### [When there are overlapping columns]


```python
try:
    joined = sh.join(dataframe_1, dataframe_2, sh.JoinStatement("x1"))
except ValueError as error:
    print(f"Error raised as expected: {error}")
    joined = sh.join(dataframe_1, dataframe_2, sh.JoinStatement("x1"), when_same_columns="left")
joined.toPandas()
```

    Error raised as expected: 
    
    Overlapping columns found in the dataframes: ['x1', 'x3', 'x4']
    Please provide the `when_same_columns` argument therefore, to select a selection strategy:
    	* "left": Use all the intersecting columns from the left dataframe
    	* "right": Use all the intersecting columns from the right dataframe
    	* [["x_in_left", "y_in_left"], ["z_in_right"]]: Provide column names for both
    





<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>x1</th>
      <th>x2</th>
      <th>x3</th>
      <th>x4</th>
      <th>x5</th>
      <th>x6</th>
      <th>x7</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>J</td>
      <td>734</td>
      <td>499</td>
      <td>595.0</td>
      <td>187</td>
      <td>431.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B</td>
      <td>J</td>
      <td>357</td>
      <td>202</td>
      <td>525.0</td>
      <td>523</td>
      <td>503.5</td>
    </tr>
    <tr>
      <th>2</th>
      <td>C</td>
      <td>H</td>
      <td>864</td>
      <td>568</td>
      <td>433.5</td>
      <td>988</td>
      <td>181.05</td>
    </tr>
    <tr>
      <th>3</th>
      <td>D</td>
      <td>J</td>
      <td>530</td>
      <td>703</td>
      <td>112.3</td>
      <td>419</td>
      <td>42.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>E</td>
      <td>H</td>
      <td>61</td>
      <td>521</td>
      <td>906.0</td>
      <td>805</td>
      <td>558.2</td>
    </tr>
    <tr>
      <th>5</th>
      <td>F</td>
      <td>H</td>
      <td>482</td>
      <td>496</td>
      <td>13.0</td>
      <td>722</td>
      <td>721.0</td>
    </tr>
  </tbody>
</table>
</div>



### [Keeping the duplicate columns from the right dataframe]


```python
joined = sh.join(dataframe_1, dataframe_2, sh.JoinStatement("x1"), when_same_columns="right")
joined.toPandas()
```




<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>x1</th>
      <th>x2</th>
      <th>x3</th>
      <th>x4</th>
      <th>x5</th>
      <th>x6</th>
      <th>x7</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>J</td>
      <td>O</td>
      <td>946</td>
      <td>595.0</td>
      <td>187</td>
      <td>431.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B</td>
      <td>J</td>
      <td>P</td>
      <td>692</td>
      <td>525.0</td>
      <td>523</td>
      <td>503.5</td>
    </tr>
    <tr>
      <th>2</th>
      <td>C</td>
      <td>H</td>
      <td>Q</td>
      <td>550</td>
      <td>433.5</td>
      <td>988</td>
      <td>181.05</td>
    </tr>
    <tr>
      <th>3</th>
      <td>D</td>
      <td>J</td>
      <td>R</td>
      <td>50</td>
      <td>112.3</td>
      <td>419</td>
      <td>42.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>E</td>
      <td>H</td>
      <td>S</td>
      <td>824</td>
      <td>906.0</td>
      <td>805</td>
      <td>558.2</td>
    </tr>
    <tr>
      <th>5</th>
      <td>F</td>
      <td>H</td>
      <td>T</td>
      <td>69</td>
      <td>13.0</td>
      <td>722</td>
      <td>721.0</td>
    </tr>
  </tbody>
</table>
</div>



### [Keeping the duplicate columns from both]


```python
joined = sh.join(
    dataframe_1, dataframe_2, sh.JoinStatement("x1"), 
    when_same_columns=[["x1", "x3"], ["x4"]]
)
joined.toPandas()
```




<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>x1</th>
      <th>x2</th>
      <th>x3</th>
      <th>x4</th>
      <th>x5</th>
      <th>x6</th>
      <th>x7</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>J</td>
      <td>734</td>
      <td>946</td>
      <td>595.0</td>
      <td>187</td>
      <td>431.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B</td>
      <td>J</td>
      <td>357</td>
      <td>692</td>
      <td>525.0</td>
      <td>523</td>
      <td>503.5</td>
    </tr>
    <tr>
      <th>2</th>
      <td>C</td>
      <td>H</td>
      <td>864</td>
      <td>550</td>
      <td>433.5</td>
      <td>988</td>
      <td>181.05</td>
    </tr>
    <tr>
      <th>3</th>
      <td>D</td>
      <td>J</td>
      <td>530</td>
      <td>50</td>
      <td>112.3</td>
      <td>419</td>
      <td>42.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>E</td>
      <td>H</td>
      <td>61</td>
      <td>824</td>
      <td>906.0</td>
      <td>805</td>
      <td>558.2</td>
    </tr>
    <tr>
      <th>5</th>
      <td>F</td>
      <td>H</td>
      <td>482</td>
      <td>69</td>
      <td>13.0</td>
      <td>722</td>
      <td>721.0</td>
    </tr>
  </tbody>
</table>
</div>



### [Complex join]


```python
x1_x1 = sh.JoinStatement("x1")
x1_x3 = sh.JoinStatement("x1", "x3")
statement = sh.JoinStatement(x1_x1, x1_x3, "or")
joined = sh.join(dataframe_1, dataframe_2, statement, when_same_columns="left")
joined.toPandas()
```




<div>

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>x1</th>
      <th>x2</th>
      <th>x3</th>
      <th>x4</th>
      <th>x5</th>
      <th>x6</th>
      <th>x7</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>J</td>
      <td>734</td>
      <td>499</td>
      <td>595.0</td>
      <td>187</td>
      <td>431.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>B</td>
      <td>J</td>
      <td>357</td>
      <td>202</td>
      <td>525.0</td>
      <td>523</td>
      <td>503.5</td>
    </tr>
    <tr>
      <th>2</th>
      <td>C</td>
      <td>H</td>
      <td>864</td>
      <td>568</td>
      <td>433.5</td>
      <td>988</td>
      <td>181.05</td>
    </tr>
    <tr>
      <th>3</th>
      <td>D</td>
      <td>J</td>
      <td>530</td>
      <td>703</td>
      <td>112.3</td>
      <td>419</td>
      <td>42.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>E</td>
      <td>H</td>
      <td>61</td>
      <td>521</td>
      <td>906.0</td>
      <td>805</td>
      <td>558.2</td>
    </tr>
    <tr>
      <th>5</th>
      <td>F</td>
      <td>H</td>
      <td>482</td>
      <td>496</td>
      <td>13.0</td>
      <td>722</td>
      <td>721.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>G</td>
      <td>A</td>
      <td>350</td>
      <td>279</td>
      <td>941.0</td>
      <td>483</td>
      <td>707.1</td>
    </tr>
    <tr>
      <th>7</th>
      <td>J</td>
      <td>A</td>
      <td>228</td>
      <td>765</td>
      <td>7.0</td>
      <td>68</td>
      <td>902.0</td>
    </tr>
  </tbody>
</table>
</div>



### [Further nested joins are not supported]
(Perform sequential joins instead)


```python
x1_x1 = sh.JoinStatement("x1")
x1_x2 = sh.JoinStatement("x1", "x3")
statement = sh.JoinStatement(x1_x1, x1_x2, "or")
statement_complex = sh.JoinStatement(statement, statement, "and")
try:
    joined = sh.join(dataframe_1, dataframe_2, statement_complex, when_same_columns="left")
except NotImplementedError as error:
    print(f"Error raised as expected: [{error}]")
```

    Error raised as expected: [Recursive JoinStatement not implemented]
