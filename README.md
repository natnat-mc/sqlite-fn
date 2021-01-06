# sqlite-fn

A sqlite3 extension to generate sql functions in code

## How to use it

### Create a function (the argc way)

```sql
SELECT create_function(function_name, arg_count, flags, body);
```
where `function_name`, `flags` and `body` are `TEXT` and `arg_count` is `INTEGER`

### Create a function (the arglist way)

```sql
SELECT create_function_v2(function_name, flags, body, ...args);
```

where `function_name`, `flags` and `body` are `TEXT` and `...args` is a comma separated list of `TEXT` representing the argument names in order

For exemple,
```sql
SELECT create_function_v2('add', 'di', 'a+b+c', 'a', 'b', 'c');
```

### Creating a reducer function

A reducer function is a function that takes a list of values (as varargs) or rows (from a `SELECT` statement) and process them into a single value.

```sql
SELECT create_reducer(name, flags, body[, accname, currname]);
```

Usage
```sql
SELECT create_reducer('sum', 'di', 'acc+curr');
SELECT sum(1, 2, 3, 4); -- 10
WITH a(a) AS (VALUES (1), (2), (3), (4), (5)) SELECT sum(a) FROM a; -- 15

SELECT create_reducer('product', 'di', 'a*b', 'a', 'b');
SELECT product(1, 2, 3, 4); -- 24
WITH b(b) AS (VALUES (1), (2), (3), (4), (5)) SELECT product(b) FROM b; -- 120
```

### Flags

There is 3 flags possible.

- `d` the function is deterministic
- `i` the function is innocuous (i.e. does not cause side effects)
- `D` the function shall only be called directly (prevents it from being used in the likes of `CHECK`, `DEFAULT` or `TRIGGER`)

## How to build it

```
git clone https://github.com/natnat-mc/sqlite-fn.git
cd sqlite-fn
make
```
