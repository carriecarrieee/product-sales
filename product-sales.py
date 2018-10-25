""" Product Sales Queries:
        
    We have two CSV files, one that represents products and another that 
    represents sales data for those products. The files have the following schemas:


    products.csv  [https://s3-us-west-1.amazonaws.com/circleup-engr-interview-public/product_sales/products.csv]

    product_id         INT
    product_name       STRING


    sales.csv [https://s3-us-west-1.amazonaws.com/circleup-engr-interview-public/product_sales/sales.csv]

    product_id         INT
    sale_date          DATE
    sale_price         DECIMAL(38, 2)
    quantity           INT
"""

import numpy as np
import pandas as pd


class ProductSales:

    # products_url = "https://s3-us-west-1.amazonaws.com/circleup-engr-interview-public/product_sales/products.csv"
    # sales_url = "https://s3-us-west-1.amazonaws.com/circleup-engr-interview-public/product_sales/sales.csv"

    products_url = "data/products.csv"
    sales_url = "data/sales.csv"

    def __init__(self):
        self.df = None


    def get_df(self):
        """ Creates df from products csv and sales csv and merges them into one.
        """

        products_df = None
        sales_df = None

        if not self.df:
            try:
                products_df = pd.read_csv(self.products_url, sep=',', \
                                            names = ['ID', 'Prod_Name'])

                sales_df = pd.read_csv(self.sales_url, sep=',', \
                                        infer_datetime_format=True, \
                                        names = ['ID', 'Date', 'Price', 'Quantity'])

                # Left join the two dfs on 'ID', which will be set as the index
                df = products_df.set_index('ID').join(sales_df.set_index('ID'))

            except:
                print("Error: No data found!")

        return df

#                                   Prod_Name        Date  Price  Quantity
# ID
# 1                             Windswell IPA  2017-01-11  13.99         1
# 1                             Windswell IPA  2017-03-09  13.99         1
# 1                             Windswell IPA  2017-03-11  13.99         1
# 1                             Windswell IPA  2017-02-05  13.99         1
# 1                             Windswell IPA  2017-01-30  13.99         2
# 1                             Windswell IPA  2017-01-09  13.99         2
# 1                             Windswell IPA  2017-02-14  13.99         2
# 1                             Windswell IPA  2017-03-07  13.99         2
# 1                             Windswell IPA  2017-03-02  13.99         2
# 1                             Windswell IPA  2017-03-19  13.99         1
# 1                             Windswell IPA  2017-03-25  13.99         2
# 1                             Windswell IPA  2017-03-17  13.99         2
# 1                             Windswell IPA  2017-01-02  13.99         2
# 1                             Windswell IPA  2017-02-11  13.99         1
# 1                             Windswell IPA  2017-03-26  13.99         5
# 1                             Windswell IPA  2017-01-05  13.99         1
# 1                             Windswell IPA  2017-03-23  13.99         1
# 1                             Windswell IPA  2017-02-10  13.99         3
# 1                             Windswell IPA  2017-03-17  13.99         1
# 1                             Windswell IPA  2017-01-15  13.99         6
# 1                             Windswell IPA  2017-01-24  13.99         1
# 1                             Windswell IPA  2017-03-30  13.99         2
# 1                             Windswell IPA  2017-03-12  13.99         2
# 1                             Windswell IPA  2017-03-05  13.99         8
# 1                             Windswell IPA  2017-01-23  13.99         2
# 1                             Windswell IPA  2017-03-28  13.99         2
# 1                             Windswell IPA  2017-02-13  13.99         7
# 1                             Windswell IPA  2017-01-05  13.99         2
# 1                             Windswell IPA  2017-03-27  13.99         2
# 1                             Windswell IPA  2017-01-27  13.99         5
# ..                                      ...         ...    ...       ...
# 11  Reese Witherspoon's All-Natural PB Cups  2017-02-18   1.99         2
# 11  Reese Witherspoon's All-Natural PB Cups  2017-01-24   1.99         2
# 11  Reese Witherspoon's All-Natural PB Cups  2017-03-01   1.99         1
# 11  Reese Witherspoon's All-Natural PB Cups  2017-02-13   1.99         5
# 11  Reese Witherspoon's All-Natural PB Cups  2017-01-20   1.99         3
# 11  Reese Witherspoon's All-Natural PB Cups  2017-02-12   1.99         1
# 11  Reese Witherspoon's All-Natural PB Cups  2017-02-02   1.99         1
# 11  Reese Witherspoon's All-Natural PB Cups  2017-01-31   1.99         4
# 11  Reese Witherspoon's All-Natural PB Cups  2017-01-10   1.99         1
# 11  Reese Witherspoon's All-Natural PB Cups  2017-01-19   1.99         2
# 11  Reese Witherspoon's All-Natural PB Cups  2017-01-25   1.99         2
# 11  Reese Witherspoon's All-Natural PB Cups  2017-02-05   1.99         2
# 11  Reese Witherspoon's All-Natural PB Cups  2017-02-10   1.99         1
# 11  Reese Witherspoon's All-Natural PB Cups  2017-03-03   1.99         1
# 11  Reese Witherspoon's All-Natural PB Cups  2017-01-06   1.99         9
# 11  Reese Witherspoon's All-Natural PB Cups  2017-03-21   1.99         1
# 11  Reese Witherspoon's All-Natural PB Cups  2017-02-03   1.99         1
# 11  Reese Witherspoon's All-Natural PB Cups  2017-02-07   1.99         2
# 11  Reese Witherspoon's All-Natural PB Cups  2017-03-21   1.99         2
# 11  Reese Witherspoon's All-Natural PB Cups  2017-02-12   1.99         2
# 11  Reese Witherspoon's All-Natural PB Cups  2017-01-24   1.99         5
# 11  Reese Witherspoon's All-Natural PB Cups  2017-02-20   1.99         2
# 11  Reese Witherspoon's All-Natural PB Cups  2017-02-13   1.99         7
# 11  Reese Witherspoon's All-Natural PB Cups  2017-03-13   1.99         1
# 11  Reese Witherspoon's All-Natural PB Cups  2017-02-23   1.99         1
# 11  Reese Witherspoon's All-Natural PB Cups  2017-02-23   1.99         2
# 11  Reese Witherspoon's All-Natural PB Cups  2017-02-27   1.99         1
# 11  Reese Witherspoon's All-Natural PB Cups  2017-02-04   1.99         2
# 11  Reese Witherspoon's All-Natural PB Cups  2017-01-05   1.99         1
# 11  Reese Witherspoon's All-Natural PB Cups  2017-01-16   1.99         1


if __name__ == "__main__":
    import doctest

    ps = ProductSales()
    print(ps.get_df())

#     result = doctest.testmod()
#     if result.failed == 0:
#         print("\nALL TESTS PASSED\n")