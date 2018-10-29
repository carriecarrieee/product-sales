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


    This program will output a dataframe with three columns: product name,
    cumulative revenue amount (total sales over the product lifetime), and the
    date on which the most revenue was generated.

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

        if self.df is None:
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

        return self.df


    def prep_data(self):
        """ Returns a dataframe with analytical data ready to be used in 
            subsequent functions.
        """

        df = self.get_df()

        # Add revenue column (price per item times the quantity)
        df['Rev'] = df['Price'] * df['Quantity']

        # Sum up rev on each day, remove ID, set prod name as index.
        df = df.groupby(['ID','Prod_Name','Date'])['Rev'] \
               .sum() \
               .reset_index() \
               .set_index(['ID']) \
               .sort_values(['Rev'], ascending=False)

        return df


    def get_dates_df(self):
        """ Returns new df that shows the dates for each product where the 
            highest revenue was generated.
        """

        df = self.prep_data()
        
        # Takes the first row from each index group
        df_dates = df.groupby(df.index).nth(0)

        return df_dates


    def get_cum_rev_df(self):
        """ Returns new df that shows the top 3 products by their cumulative
            revenue (total sales over the product lifetime.
        """

        df = self.prep_data()

        # Show top 3 products by their cumulative revenue
        df_cum_rev = df.groupby(['ID','Prod_Name'], sort=False)['Rev'] \
                       .sum() \
                       .reset_index() \
                       .set_index(['ID']) \
                       .sort_values(['Rev'], ascending=False) \
                       .iloc[:3]

        return df_cum_rev


    def join_dfs(self):
        """ Returns a combined df based on two dfs--one that shows dates on
            which highest revenue was generated, and one that shows the top 3
            products by cumulative revenue over the product lifetime.
        """

        df_dates = self.get_dates_df()
        df_cum_rev = self.get_cum_rev_df()

        # Perform inner join on the index with the two dfs
        combined = df_cum_rev.join(df_dates, how="inner", lsuffix="_Cum")

        # Reorder and keep only relevant columns, rename column header
        final_df = combined[['Prod_Name','Rev_Cum','Date']] \
                    .rename(columns={'Date': 'Date_of_Highest_Rev', \
                                     'Rev_Cum': 'Cumulative_Rev'}) \
                    .set_index('Prod_Name')

        return final_df



if __name__ == "__main__":

    ps = ProductSales()
    print(ps.join_dfs())

