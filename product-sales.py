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
                                            names = ['id', 'prod_name'])
                sales_df = pd.read_csv(self.sales_url, sep=',', \
                                        infer_datetime_format=True, \
                                        names = ['id', 'date', 'price', 'quantity'])

            except:
                print("Error: No data found!")

        return sales_df


if __name__ == "__main__":
    import doctest

    ps = ProductSales()
    print(ps.get_df())

#     result = doctest.testmod()
#     if result.failed == 0:
#         print("\nALL TESTS PASSED\n")