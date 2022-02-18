
import json
import pandas as pd
from bs4 import BeautifulSoup
import numpy
# import the modules
from pymysql import connect
# import xlwt
import pandas.io.sql as sql
# connect the mysql with the python
import json


class LeadUtilizationReport:

    def __init__(self):
        # self.connection(user, password, host, database)
        self.read_json_file()

        # self.main_df()

        # self.select_server(1)

    def list_servers(self):
        return self.servers

    def read_json_file(self):
        with open("servers.json", "r") as file:
            # print(json.load(file))
            self.servers = json.load(file)

    def select_server(self, id):

        for db_config in self.servers:
            if db_config['id'] == id:
                self.con = connect(host=db_config['host'], database=db_config['database'],
                                   user=db_config['user'], password=db_config['password'])

        list_of_ids_in_server = self.list_of_ids()
        # print(list_of_ids_in_server)
        return list_of_ids_in_server

    def main_df(self):
        df1 = sql.read_sql(
            'SELECT * FROM vicidial_list WHERE STATUS = "DROP" and called_count=(SELECT MIN(called_count) FROM vicidial_list WHERE STATUS = "DROP");', self.con)
        print(df1)

    def list_of_ids(self):

        list_of_ids_list = []

        all_lists = sql.read_sql(
            'SELECT DISTINCT list_id FROM vicidial_list ORDER BY list_id DESC;', self.con)
        # print(type(all_lists))
        print(all_lists.to_dict())

        for key, value in all_lists.to_dict()['list_id'].items():
            list_of_ids_list.append({
                "list_id": value
            })

        return list_of_ids_list

    def single_select(self, list_id, server_id):

        self.select_server(server_id)

        max_called = """SELECT MAX(called_count) FROM vicidial_list WHERE list_id = {list_idd};""".format(
            list_idd=list_id)
        maxx = sql.read_sql(max_called, self.con)
        maxx = maxx.iat[0, 0]

        query = ""
        for iter in range(maxx+1):
            query = query + \
                "COUNT(CASE called_count WHEN '{count}' THEN 1 ELSE NULL END) AS '{count}',".format(
                    count=iter)

        queryx = """SELECT `status` AS `STATUS`,""" + query + \
            """COUNT(*) AS SUBTOTAL FROM vicidial_list WHERE list_id = {list_idd} GROUP BY `status`;""".format(
                list_idd=list_id)

        df3 = sql.read_sql(queryx, self.con)

        df3_length = len(df3)
        df3.loc["Total"] = df3.sum()
        df3.loc['Total', 'STATUS'] = 'Total'

        total_runs = []
        for total_iter in range(maxx+1):
            total_runs.append(total_iter*(df3.iloc[-1][total_iter+1]))
        # print(type(total_runs[1]))
        total_runs.append(
            sum(filter(lambda i: isinstance(i, numpy.int64), total_runs)))
        total_runs.insert(0, 'Total Runs')

        total_runs_value = int(total_runs[-1])
        df3_last_value = int(df3[df3.columns[-1]].to_list()[-1])

        
        df3.loc[df3_length] = total_runs

        soup = BeautifulSoup(df3.to_html(index=False), "html.parser")

        print(soup)


        print("Check 1\n",df3)

        # total_runs_array = numpy.array(total_runs, dtype=object)
        # total_runs_df = pd.DataFrame(total_runs_array)

        # total_runs_html = total_runs_df.T.to_html(index=False, header=None)

        # print(total_runs_html)

        # print("Type of total runs is: ",type(total_runs))

        return {
            "html": str(soup).replace("\n",""),
            # "total_runs": str(total_runs_html),
            "total_runs_value": total_runs_value,
            "dataframe_last_value": df3_last_value
        }

    # def percentage()


if __name__ == "__main__":

    server1 = LeadUtilizationReport()
    server1.select_server(1)
    # server1.main_df()
    server1.list_of_ids()

    print(server1.single_select())
