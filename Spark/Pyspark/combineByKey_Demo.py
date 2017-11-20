# CombineByKey() is used when we need to have some preprocessing logic to get the zero value passed to the accumulator.
# This is done by passing in a function which gets the value and the return value of this function is used as zero value passed to each combiner.
# This is not possible in aggregateByKey where we can directly specify the value.

