import pandas as pd

print("this program reads empdata.txt")


def readTxtIntoDataFrame(file_path):
    df = pd.read_csv(file_path)
    return df


def sumofsalary_by_deptNam(df):
    newdf = df.groupby('dept_name')['salary'].sum().reset_index()
    return newdf


if __name__ == "__main__":
    file_path = input("enter file: ")

    dataframe = readTxtIntoDataFrame(file_path)
    print(dataframe)
    result = sumofsalary_by_deptNam(dataframe)

    print("sum of salary by department: ")

    print(result)
