# 第一题
结果如图
![](img/showVersionSql.png)

# 第二题
## 优化规则解释
### CombineFilters
官方解释：
> Combines two adjacent [[Filter]] operators into one, merging the non-redundant conditions into one conjunctive predicate.

简单讲就是合并filter操作，条件是这两个filter是不关注顺序切实切实确定的（deterministic = true），属于算子合并。比如：
> select * from (select name,age from student where age>10) where age < 20

### CollapseProject
官方解释
>  Combines two [[Project]] operators into one and perform alias substitution, merging the expressions into one single expression for the following cases.
>  1. When two [[Project]] operators are adjacent.
>  2. When two [[Project]] operators have LocalLimit/Sample/Repartition operator between them 
      and the upper project consists of the same number of columns which is equal or aliasing.`GlobalLimit(LocalLimit)` pattern is also considered.

合并project，需要这2个project是相邻的，2个project操作之间有LocalLimit/Sample/Repartition且列是相同的。比如：
> select name from ( select name,age from student)

### BooleanSimplification
官方解释：
> Simplifies boolean expressions:
> 1. Simplifies expressions whose answer can be determined without evaluating both sides.
> 2. Eliminates / extracts common factors.
> 3. Merge same expressions
> 4. Removes `Not` operator.