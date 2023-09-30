#importing necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, format_number, col


# defining what goes into our main construct
if __name__ == "__main__":
    
    #getting the sparkcontext from the sparksession and setting log level
    session = SparkSession.builder.appName('emretl').getOrCreate()
    sc = session.sparkContext
    sc.setLogLevel('ERROR')
    
    #creating a dataframe reader
    dataframereader = session.read
    sales_data = dataframereader \
        .option("header", "true") \
        .option("inferSchema", value = True) \
        .csv("s3://firstemrbucket/sales_data.csv") #replace firstemrbucket with the actual name of your s3 bucket
    
    #printing out the schema of our data
    print("===Schema ===")
    sales_data.printSchema()

    #previewing top 20 rows of our data
    sales_data.show()
    
    #checking the data types of the various columns
    sales_data.dtypes
    
    #specifying selected columns and the data type we want to cast them to
    columns_to_cast = {
    "sales": "int", 
    "quantity": "int",
    "discount": "double",
    "profit": "double",
    "shipping_cost": "double"    
}
    
    #casting columns using a simple for loop
    for column, data_type in columns_to_cast.items():
        if column in sales_data.columns:
            sales_data = sales_data.withColumn(column, col(column).cast(data_type))
            
    
    #showing the total sales and profit for each market
    print("=======Showing the amount of sales for each market in descending order======" )
    sales_data.groupBy("market") \
        .agg(
            sum("sales").alias("Total_Sales"), \
            sum("profit").alias("Total_Profit") \
        ).orderBy("Total_Profit", ascending=False).show()


    #getting the total sales and profit for each market at the sub-category level
    profit_and_sales_at_sub_cat = \
    (sales_data.groupBy("market","sub_category") 
        .agg(
            sum("quantity").alias("Number_Of_Items"),
            sum("sales").alias("TotalSales"), 
            sum("profit").alias("Profit_Made")
        ) 
        .orderBy("Profit_Made", ascending=False))

    #showing the total profit and sales for each market at the sub category level
    profit_and_sales_at_sub_cat.show()

    
    #saving our output to S3
    print("=======Saving the amount of sales and profit for each market at the sub category level======" )
     
        
    (profit_and_sales_at_sub_cat.write 
    .mode("overwrite")  
    .csv("s3://firstemrbucket/profit_and_sales_at_sub_cat")) #replace firstemrbucket with the actual name of your s3 bucket

