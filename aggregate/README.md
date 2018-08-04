# Functions

### Aggregation Functions


#### as_list
```
as_list(expr)
as_list(expr,max_count)
```

Aggregates all rows of a column into a single array. The argument *max_count*
specifies the maximum length. If this parameter is omitted, a reasonable default is choosen.

#### first
```
first(expr)
```

Aggregates all rows of a column by picking the first one.

#### last
```
last(expr)
```

Aggregates all rows of a column by picking the last one.

#### filter
```
filter(aggr,cond)
```

This function applies the given aggregation function *aggr* only on those rows, specified by *cond*.

#### group_by
```
group_by(key,aggr)
group_by(key,aggr,max_count)
```

This function works like a mixture of `GROUP BY` and `as_list(...)`.

This function applies the given aggregation function *aggr* grouped by *key*. The result is returned as array.
The argument *max_count* specifies the maximum length. If this parameter is omitted, a reasonable default is choosen.

This function is to be used instead of `as_list(...)`, if the user wishes a deduplicated array:

```
group_by(my_column,first(my_column))
```

### Regular functions

#### dict

```
dict(key1,value1,key2,value2,...)
```

This function creates a JSON-Style Object. Names and values must be interleaved.


#### array

```
array(value1,value2,value3)
```

This function creates a JSON-Style Array.

