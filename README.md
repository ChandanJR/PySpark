# PySpark Task

Sales - analysis

i.Extract:  Load the data


    - Read data as pandas dataframe and then create spark dataframe
    
    
ii.Transform: Exploratory data analysis using spark df


    - Data preprocessing in spark df
    
    
       replace blank/null/empty string values to "NA"
       
       
       filter - country by USA
       
       
       using broadcast replace state NY -> Newyork, CA -> California, and so on
       
       
    - Unique order count
    
    
    - calculate delivery_cost column from (quantityordered*priceeach) - sales)
    
    
    - GroupBy country and avg of priceeach,quantityordered
    
    
iii.Load: Save analysis report


    - GroupBy country and avg of priceeach,quantityordered save as files
