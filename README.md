# Pyspark Helper Functions
### [For less verbose and fool-proof operations]


```python
try:
    from pyspark import SparkConf
except ImportError:
    ! pip install pyspark==3.2.1

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import types as st

import spark.helpers as sh
from spark.join import JoinValidator, JoinStatement
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
_ = dataframe_1.show(), dataframe_2.show()
```

    +---+---+---+---+-----+
    | x1| x2| x3| x4|   x5|
    +---+---+---+---+-----+
    |  A|  J|734|499|595.0|
    |  B|  I|357|202|525.0|
    |  C|  H|864|568|433.5|
    |  D|  G|530|703|112.3|
    |  E|  F| 61|521|906.0|
    |  F|  E|482|496| 13.0|
    |  G|  D|350|279|941.0|
    |  H|  C|171|267|423.0|
    |  I|  B|755|133|600.0|
    |  J|  A|228|765|  7.0|
    +---+---+---+---+-----+
    
    +---+---+---+---+------+
    | x1| x3| x4| x6|    x7|
    +---+---+---+---+------+
    |  W|  K|391|140| 872.0|
    |  X|  G| 88|483| 707.1|
    |  Y|  M|144|476| 714.3|
    |  Z|  J|896| 68| 902.0|
    |  A|  O|946|187| 431.0|
    |  B|  P|692|523| 503.5|
    |  C|  Q|550|988|181.05|
    |  D|  R| 50|419|  42.0|
    |  E|  S|824|805| 558.2|
    |  F|  T| 69|722| 721.0|
    +---+---+---+---+------+
    


## Pandas-like group by


```python
for group, data in sh.group_iterator(dataframe_1, "x1"):
    print(group, data.toPandas().shape[0])
```

    A 1
    B 1
    C 1
    D 1
    E 1
    F 1
    G 1
    H 1
    I 1
    J 1


## Bulk-change schema


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

## Improved joins


```python
joined = sh.join(dataframe_1, dataframe_2, JoinStatement("x1", "x1"))
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
      <td>I</td>
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
      <td>G</td>
      <td>530</td>
      <td>703</td>
      <td>112.3</td>
      <td>419</td>
      <td>42.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>E</td>
      <td>F</td>
      <td>61</td>
      <td>521</td>
      <td>906.0</td>
      <td>805</td>
      <td>558.2</td>
    </tr>
    <tr>
      <th>5</th>
      <td>F</td>
      <td>E</td>
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
joined = sh.join(dataframe_1, dataframe_2, JoinStatement("x1", "x1"), duplicate_keep="right")
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
      <td>I</td>
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
      <td>G</td>
      <td>R</td>
      <td>50</td>
      <td>112.3</td>
      <td>419</td>
      <td>42.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>E</td>
      <td>F</td>
      <td>S</td>
      <td>824</td>
      <td>906.0</td>
      <td>805</td>
      <td>558.2</td>
    </tr>
    <tr>
      <th>5</th>
      <td>F</td>
      <td>E</td>
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
    dataframe_1, dataframe_2, JoinStatement("x1", "x1"), 
    duplicate_keep=[["x1", "x3"], ["x4"]]
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
      <td>I</td>
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
      <td>G</td>
      <td>530</td>
      <td>50</td>
      <td>112.3</td>
      <td>419</td>
      <td>42.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>E</td>
      <td>F</td>
      <td>61</td>
      <td>824</td>
      <td>906.0</td>
      <td>805</td>
      <td>558.2</td>
    </tr>
    <tr>
      <th>5</th>
      <td>F</td>
      <td>E</td>
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
x1_x1 = JoinStatement("x1", "x1")
x1_x3 = JoinStatement("x1", "x3")
statement = JoinStatement(x1_x1, x1_x3, "or")
joined = sh.join(dataframe_1, dataframe_2, statement)
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
      <td>I</td>
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
      <td>G</td>
      <td>530</td>
      <td>703</td>
      <td>112.3</td>
      <td>419</td>
      <td>42.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>E</td>
      <td>F</td>
      <td>61</td>
      <td>521</td>
      <td>906.0</td>
      <td>805</td>
      <td>558.2</td>
    </tr>
    <tr>
      <th>5</th>
      <td>F</td>
      <td>E</td>
      <td>482</td>
      <td>496</td>
      <td>13.0</td>
      <td>722</td>
      <td>721.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>G</td>
      <td>D</td>
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
x1_x1 = JoinStatement("x1", "x1")
x1_x2 = JoinStatement("x1", "x3")
statement = JoinStatement(x1_x1, x1_x2, "or")
statement_complex = JoinStatement(statement, statement, "and")
try:
    joined = sh.join(dataframe_1, dataframe_2, statement_complex)
except NotImplementedError as error:
    print(f"Error raised as expected: [{error}]")
```

    Error raised as expected: [Recursive JoinStatement not implemented]

