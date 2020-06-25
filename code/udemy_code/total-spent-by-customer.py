import base as bs


def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

input = bs.sc.textFile(bs.basepath+"customer-orders.csv")
mappedInput = input.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)

results = totalByCustomer.collect();
for result in results:
    print(result)

# to sort the amount
# sortedRDD = totalByCustomer.sortBy(lambda a: a[1], ascending=False)

# sortedRes = sortedRDD.collect()
# for val in sortedRes:
#     print(val)