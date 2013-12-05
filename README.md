hive-ruby-scripting
===================
Ruby scripting for Apache Hive
http://labs.gree.jp/blog/2013/12/9061/

*Hive 0.12.0, JRuby 1.7.4, JDK7 is recommended.*

## build
by using `mvn package`

## setup
```
add jar hdfs:///path/to/jruby.jar;
add jar hdfs:///path/to/hive-ruby-scripting.jar;

create temporary function rb_exec as 'net.gree.hive.GenericUDFRubyExec';
create temporary function rb_inject as 'net.gree.hive.GenericUDAFRubyInject';
```

## rb_exec
#### Evaluate using ruby scriptlet
```sql
select foo, rb_exec('"<" + @arg1.to_s + ">"', foo ) from pokes;
```

#### Evalute using method defined in `rb.script`
```sql
set rb.script=
def hello (name)
  "hello, " + name
end
;

select rb_exec('&hello', name) from user;
```

#### Return non-string results
```sql
-- return Map<String, String> by passing hint as 1st argument

select rb_exec(Map('k','v'), '{@arg1 => @arg2}', name, score) from scores;
```

## rb_inject
#### do Aggregation by using ruby scriplet
```sql
-- it's same as `select sum(coin) from user_coin group by user`

set rb.script =  
def sum(memo, arg)  
  memo + arg  
end  
;
 
select
  user,  
  rb_inject('sum', coin) 
from user_coin  
group by user
;
```

## License
MIT License ([More Information](http://en.wikipedia.org/wiki/MIT_License))
