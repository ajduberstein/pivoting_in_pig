import collections
from org.apache.pig.scripting import *
input_path = 'tmp.txt' #Set to whatever your input path and filename are
#First, we run an embedded job to find all the distinct levels of model and make
find_distincts = """
A = LOAD '$INPUT' USING PigStorage() AS (user:chararray
        , year:chararray
        , make:chararray
        , model:chararray
        , mileage:chararray);
B = FOREACH A GENERATE make, model;
C = DISTINCT B;
DUMP C;
"""
P = Pig.compile(find_distincts)
output = P.bind({'INPUT':input_path}).runSingle()
#Gather the models and makes from the output of the Pig script
cars = []
CarRecord = collections.namedtuple('CarRecord', 'make model')
for x in output.result("C").iterator():
        cars.append(CarRecord(make=x.get(0),model=x.get(1)))
#Next, we create a series of conditionals based off these distinct values
pivot_str = ""
cut_str = ""
#List of filters
for car in cars:
        cut_str += "%s_%s_cut" % car + "= FOREACH A GENERATE (make == '%s' AND model == '%s'" % car + "?mileage:0) AS mileage;"
#Output schema for rows we grouped by
pivot_str += "GENERATE FLATTEN(group.user) AS user, FLATTEN(group.year) AS year"
#Output schema for columns we grouped by
for car in cars:
        pivot_str += ', FLATTEN(%s_%s_cut.mileage)' % car + ' AS %s_%s_mileage' % car
pivot_str += ';'
#If you stopped the script here, it almost works--
#this approach yields duplicate records, so we have to enact a DISTINCT.
#It also produces every element of a (user,year) set, not just the
#intersection. To solve this, I sum the rows and keep only the greatest row.
sum_str = 'FOREACH C GENERATE user.., (%s_%s_mileage' % cars[0]
for car in cars[1:]:
        sum_str += ' + %s_%s_mileage' % car
sum_str += ') AS user_year_sum;'
car_str = "%s_%s_mileage" % cars[0]
for car in cars[1:]:
        car_str += ", %s_%s_mileage" % car
car_str += ';'
create_pivot = """
A = LOAD '$INPUT' USING PigStorage() AS (user:chararray
        , year:chararray
        , make:chararray
        , model:chararray
        , mileage:float);
B = FOREACH (GROUP A BY (user, year)){
        %s
        %s
};
C = DISTINCT B;
D = %s
E = GROUP D BY (user, year);
F = FOREACH E GENERATE group.user, group.year, MAX(D.user_year_sum) AS greatest;
G = JOIN F BY (user, year, greatest), D BY (user, year, user_year_sum);
out = FOREACH G GENERATE F::user AS user, F::year AS year, %s
rmf pivoted_results;
STORE out INTO 'pivoted_results';
DESCRIBE out;
""" % (cut_str,pivot_str,sum_str,car_str)
print create_pivot
create_pivot_P = Pig.compile(create_pivot)
output = create_pivot_P.bind({'INPUT':input_path}).runSingle()
