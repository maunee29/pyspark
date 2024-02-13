import argparse

from pyspark.sql.types import IntegerType

a = 10
b = 2
def addmynum(a, b):
    return a + b

def divmynum(a, b):
    return a/b

if __name__ == "__main":
    parser = argparse.ArgumentParser(description="Testing of Pything code")
    parser.add_argument("input_num1", help="Enter First Number")
    parser.add_argument("input_num2", help="Enter Second Number")

    args = parser.parse_args()
    myresult = addmynum(IntegerType(args.input_num1, IntegerType(args.input_num2)))
    myresult2 = divmynum(int(args.input_num1), int(args.input_num2))
    print(myresult)
    print(myresult2)
