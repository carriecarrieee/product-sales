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
                self.df = products_df.set_index('ID').join(sales_df.set_index('ID'))

            except:
                print("Error: No data found!")

        print(type(self.df))
        return self.df


    def get_top_3_products(self):
        """ Returns the top 3 products by cumulative revenue (total sales over 
            the product lifetime).
        """

        df = self.get_df()

        # Add revenue column (price per item times the quantity)
        df['Rev'] = df['Price'] * df['Quantity']

        # Sum up rev to get cumulative rev and group by product; sort to find
        # the top 3 revenue-generating products.
        df = df.groupby(['Prod_Name'])['Rev'] \
               .sum() \
               .reset_index() \
               .set_index(['Prod_Name']) \
               .sort_values('Rev', ascending=False)

        return df.iloc[:3]



if __name__ == "__main__":
    import doctest

    ps = ProductSales()
    print(ps.query_top_3_products())

#     result = doctest.testmod()
#     if result.failed == 0:
#         print("\nALL TESTS PASSED\n")