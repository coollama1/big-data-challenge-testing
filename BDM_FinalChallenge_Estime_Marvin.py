import sys
from pyspark import SparkContext

sc = SparkContext()

def mapper1(partId,records):
    #if partId==0:
    #    next(records)
    import csv
    reader = csv.reader(records)
    if partId==0:
        next(records)
    for row in reader:
        if len(row) > 24:
            date = row[4].split("/")
            if len(date) > 2:
                year = date[2]
                if year >= '2015' and year <= '2019':
                    street_name = row[24]
                    if street_name:
                        street_name = street_name.upper()
                    if '-' in row[23]:
                        house_number = tuple(row[23].split('-'))
                        yield((house_number, street_name, row[21], year),1)
                    else:
                        yield((row[23], street_name, row[21], year),1)
                        #((house_num,street_name,violation_county,year), count )

def mapper2(partId,records):
    for row in records:
        key,count = row
        house_num,street_name,violation_county,year = key
        yield ((street_name,violation_county),(house_num,year,count))
        #((street_name, violation_county), (house_num, year, count))

def mapper3(partId,records):
    import csv
    reader = csv.reader(records)
    if partId==0:
        next(records)
    for row in reader:
        if len(row) > 28:
            l_low_hn = row[1]
            l_high_hn = row[3]
            r_low_hn = row[4]
            r_high_hn = row[5]
            full_stree = row[28]
            st_label = row[10]
            if full_stree:
                full_stree = full_stree.upper()
            if st_label:
                st_label = st_label.upper()
            if '-' in l_low_hn:
                l_low_hn = tuple(l_low_hn.split('-'))
                l_high_hn = tuple(l_high_hn.split('-'))
            if '-' in r_low_hn:
                r_low_hn = tuple(r_low_hn.split('-'))
                r_high_hn = tuple(r_high_hn.split('-'))
            yield(row[2], full_stree, st_label, row[13], l_low_hn,  l_high_hn, r_low_hn, r_high_hn)
            #(physicalid, full_stree, st_label, borocode, l_low_hn, l_high_hn, r_low_hn, r_high,hn)

def mapper4(partId,records):
    for row in records:
        boro_codes = {'1':'NY','2':'BX','3':'K','4':'QN','5':'ST'}
        boro = boro_codes[row[3]]
        if row[0] == row[1]:
            yield ((row[1],boro),(row[0],row[4],row[5],row[6],row[7]))
        else:
            yield ((row[1],boro),(row[0],row[4],row[5],row[6],row[7]))
            yield ((row[2],boro),(row[0],row[4],row[5],row[6],row[7]))
            #((street_name,boro), (physicalid,l_low_hn,l_high_hn,r_low_hn,r_high_hn) )
    
def mapper5(partId,records):
    for row in records:
        key,val = row
        if len(val) == 5:
            physicalid,l_low_hn,l_high_hn,r_low_hn,r_high_hn = val
            counts_per_year = [0,0,0,0,0]
            yield (physicalid, counts_per_year)
        elif len(val) == 2:
            violation,street = val
            house_num, year, count = violation
            physicalid,l_low_hn,l_high_hn,r_low_hn,r_high_hn = street
            valid_hn = False
            if l_low_hn and r_low_hn:
                if isinstance(l_low_hn, str) and isinstance(r_low_hn,str) and isinstance(house_num,str):
                    house_num_int = int(house_num)
                    l_low_int = int(l_low_hn)
                    l_high_int = int(l_high_hn)
                    r_low_int = int(r_low_hn)
                    r_high_int = int(r_high_hn)
                    if (house_num_int % 2) == 0:
                        valid_hn = (house_num_int <= r_high_int) and (house_num_int >= r_low_int)
                    else:
                        valid_hn = (house_num_int <= l_high_int) and (house_num_int >= l_low_int)
                else:
                    l_low_1, l_low_2 = ("0","0")
                    l_high_1, l_high_2 = ("0","0")
                    r_low_1, r_low_2 = ("0","0")
                    r_high_1, r_high_2 = ("0","0")
                    house_1, house_2 = ("0","0")
                    house_1_int = 0
                    house_2_int = 0
                    
                    if not isinstance(l_low_hn,str):
                        l_low_1, l_low_2 = l_low_hn
                        l_high_1, l_high_2 = l_high_hn
                    
                    if not isinstance(l_low_hn,str):
                        r_low_1, r_low_2 = r_low_hn
                        r_high_1, r_high_2 = r_high_hn
                    
                    if not isinstance(house_num,str):
                        house_1, house_2 = house_num
                        house_1_int = int(house_1)
                        house_2_int = int(house_2)
                    
                    l_low_1_int = int(l_low_1)
                    l_low_2_int = int(l_low_2)
                    l_high_1_int = int(l_high_1)
                    l_high_2_int = int(l_high_2)
                    r_low_1_int = int(r_low_1)
                    r_low_2_int = int(r_low_2)
                    r_high_1_int = int(r_high_1)
                    r_high_2_int = int(r_high_2)

                    if house_num and not (house_1_int == 0 and house_2_int == 0):
                        if (house_2_int % 2) == 0:
                            valid_hn = (house_1_int >= r_low_1_int) and (house_2_int >= r_low_2_int) and \
                                        (house_1_int <= r_high_1_int) and (house_2_int <= r_high_2_int) 
                        else:
                            valid_hn = (house_1_int >= l_low_1_int) and (house_2_int >= l_low_2_int) and \
                                        (house_1_int <= l_high_1_int) and (house_2_int <= l_high_2_int) 
            year_to_index = {"2015":0,"2016":1,"2017":2,"2018":3,"2019":4}
            index = -1
            counts_per_year = [0,0,0,0,0]
            if year <= "2019" and year >= "2015" and valid_hn:
                index = year_to_index[year]
                counts_per_year[index] = count
            yield (physicalid,counts_per_year)

def mapper6(partId,records):
    for row in records:
        physicalid, counts = row
        count_2015, count_2016, count_2017, count_2018, count_2019 = counts
        yield(physicalid,count_2015, count_2016, count_2017, count_2018, count_2019 )

def combineCounts(previous_counts, next_counts):
    output_counts= [0,0,0,0,0]
    for i in range(len(previous_counts)):
        output_counts[i] = previous_counts[i] + next_counts[i]
    return output_counts

def reducer1(perviousVal, nextVal):
    return perviousVal + nextVal

def reducer2(previousVal, nextVal):
    return combineCounts(previousVal,nextVal)

if __name__=='__main__':
    rdd = sc.textFile('/home/cusp/mestime000/big-data-challenge-testing/2015-mock.csv')
    rdd2 = sc.textFile('/home/cusp/mestime000/big-data-challenge-testing/2016-mock.csv')
    rdd3 = sc.textFile('/home/cusp/mestime000/big-data-challenge-testing/2017-mock.csv')
    rdd4 = sc.textFile('/home/cusp/mestime000/big-data-challenge-testing/2018-mock.csv')
    rdd5 = sc.textFile('/home/cusp/mestime000/big-data-challenge-testing/2019-mock.csv')
    rdd6 = sc.textFile('/home/cusp/mestime000/big-data-challenge-testing/Centerline-mock.csv')

    output_file = "output_folder"
    if len(sys.argv) >1:
        output_file = sys.argv[1]

    rdd = rdd.mapPartitionsWithIndex(mapper1).reduceByKey(reducer1)
    rdd2 = rdd2.mapPartitionsWithIndex(mapper1).reduceByKey(reducer1)
    rdd3 = rdd3.mapPartitionsWithIndex(mapper1).reduceByKey(reducer1)
    rdd4 = rdd4.mapPartitionsWithIndex(mapper1).reduceByKey(reducer1)
    rdd5 = rdd5.mapPartitionsWithIndex(mapper1).reduceByKey(reducer1)
    center_line = rdd6.mapPartitionsWithIndex(mapper3).mapPartitionsWithIndex(mapper4).distinct()

    violations = rdd.union(rdd2).reduceByKey(reducer1)\
                    .union(rdd3).reduceByKey(reducer1)\
                    .union(rdd4).reduceByKey(reducer1)\
                    .union(rdd5).reduceByKey(reducer1)\
                    .mapPartitionsWithIndex(mapper2)

    output = violations.join(center_line).union(center_line.subtractByKey(violations))
    output = output.mapPartitionsWithIndex(mapper5).reduceByKey(reducer2)\
                .mapPartitionsWithIndex(mapper6).sortBy(lambda x: x[0]).saveAsTextFile(output_file)