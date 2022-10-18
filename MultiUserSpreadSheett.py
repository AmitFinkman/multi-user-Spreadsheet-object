import datetime
import random
import sys
import threading
import tkinter as tk

import pandas as pd
from threading import *
from concurrent.futures import ThreadPoolExecutor
import time
import csv
import concurrent.futures

import numpy as np

""" our strategy is to use intended semaphore for each cell in our spread sheet """


class SharableSpreadSheet:
    data = None

    def __init__(self, nRows, nCols):
        # self.data = [nRows][nCols]
        self.data = [["" for _ in range(nCols)] for _ in range(nRows)]

        self.n = nRows if nRows >= nCols else nCols
        self.read_col_row = [threading.Semaphore(100) for _ in range(self.n + 1)]
        self.write_col_row = [threading.Semaphore(1) for _ in range(self.n + 1)]
        self.readCount = [0 for _ in range(self.n + 1)]
        self.flag = 0
        self.check = False

    def get_cell(self, row, cols):
        conditions = [row >= len(self.data), row < 0, cols < 0, cols >= len(self.data[0])]
        if any(conditions) is True:
            return None
        res = 0
        if self.flag == 0:  ## read row
            idx = row
        else:
            idx = cols
        while True:
            self.read_col_row[idx].acquire()  # wait on read semaphore
            self.readCount[idx] += 1  # increase count for reader by 1

            if self.readCount[idx] >= 1:  # since reader is present, prevent writing on data
                self.write_col_row[idx].acquire()  # wait on write semaphore
                res = self.data[row][cols]
                # time.sleep(0.1)
            # print(f"Reader {self.readCount[idx]} is reading")
            self.readCount[idx] = 0  # reading performed by reader hence decrementing readercount

            if self.readCount[idx] == 0:  # if no reader is present allow writer to write the data
                self.write_col_row[idx].release()  # signal on write semaphore, now writer can write

            self.read_col_row[idx].release()  # sinal on read semaphore
            # time.sleep(1)
            return res

    def set_cell(self, row, col, string):
        conditions = [row >= len(self.data), row < 0, col < 0, col >= len(self.data[0])]
        if any(conditions) is True:
            return False
        # set the string at [row,col]
        if self.flag == 0:  ## read row
            idx = row
        else:
            idx = col
        while True:
            self.write_col_row[idx].acquire()  # wait on write semaphore

            # print("Wrting data.....")  # write the data
            # print("-" * 20)
            self.data[row][col] = string
            # time.sleep(0.1)
            self.write_col_row[idx].release()  # signal on write semaphore
            # time.sleep(1)
            return True

    def search_string(self, str_to_search):
        # check if we need to do any pre action
        for row in range(len(self.data)):
            for col in range(
                    len(self.data[0])):  # for each cell, first read it and than check if its equal to str_to_search
                out = self.get_cell(row, col)
                if out == str_to_search:  # if found return it's index
                    return [row , col]
        return [-1, -1]

    def exchange_rows(self, row1, row2):
        # exchange the content of row1 and row2
        conditions = [row1 >= len(self.data), row1 < 0, row2 < 0, row2 >= len(self.data)]
        if any(conditions) is True:
            # if one or more from conditions is happening
            return False
        count = 0
        if self.check is True:
            while self.check:
                count += 1
        else:
            self.check = True
        self.write_col_row[row1].acquire()  # wait on write semaphore
        self.write_col_row[row2].acquire()  # wait on write semaphore
        self.flag = 0
        row_a = self.get_row(row1)  # read first row
        row_b = self.get_row(row2)  # read second row
        # row_a = [""]*(len(self.data))  # read second row
        # row_b = [""]*(len(self.data))  # read second row

        for col in range(len(self.data[0])):
            # print((row1,col))
            self.data[row1][col] = row_b[col]  # than set it

        for col in range(len(self.data[0])):
            self.data[row2][col] = row_a[col]  # than set it

        # print("Wrting data.....")  # write the data
        # print("-" * 20)

        self.write_col_row[row1].release()
        self.write_col_row[row2].release()

        self.check = False

        return True

    def get_row(self, row):  # helper to get all row
        res = []
        for val in range(0, len(self.data[0])):
            # print(
            #     f"the num of row,col is {(row, val)} , the len of rows are {len(self.data)} and columns {len(self.data[0])} ")

            res.append(self.data[row][val])

        return res

    def exchange_cols(self, col1, col2):
        # exchange the content of row1 and row2
        conditions = [col1 >= len(self.data[0]), col1 < 0, col2 < 0, col2 >= len(self.data[0])]
        if any(conditions) is True:
            return False
        count = 0
        if self.check is True:
            while self.check:
                count += 1
                # print(count)
        else:
            self.check = True
        self.write_col_row[col1].acquire()  # wait on write semaphore
        self.write_col_row[col2].acquire()  # wait on write semaphore

        self.flag = 1
        col_a = self.get_col(col1)  # read first row
        col_b = self.get_col(col2)  # read second row

        for row in range(len(self.data)):
            self.data[row][col1] = col_b[row]  # than set it

        for row in range(len(self.data)):
            self.data[row][col2] = col_a[row]  # than set it

        # print("Wrting data.....")  # write the data
        # print("-" * 20)

        self.write_col_row[col1].release()
        self.write_col_row[col2].release()
        self.check = False
        return True

    def get_col(self, col):  # helper to get all col
        res = []
        for val in range(len(self.data)):
            # print(val)
            # print(col)
            res.append(self.data[val][col])
        return res

    def search_in_row(self, row_num, str_to_search):
        conditions = [row_num >= len(self.data), row_num < 0]
        if any(conditions) is True:
            return False
        for col in range(len(self.data[0])):
            self.flag = 0
            out = self.get_cell(row_num, col)
            if out == str_to_search:
                return col
        # return -1 otherwise
        return -1

    def search_in_col(self, col_num, str_to_search):
        conditions = [col_num >= len(self.data[0]), col_num < 0]
        if any(conditions) is True:
            return False
        for row in range(len(self.data)):
            self.flag = 1
            out = self.get_cell(row, col_num)
            if out == str_to_search:
                return row
        # perform search in specific col, return row number if exists.
        # return -1 otherwise
        return -1

    def search_in_range(self, col1, col2, row1, row2, str_to_search):
        # perform search within specific range: [row1:row2,col1:col2]
        # includes col1,col2,row1,row2
        # return the first cell that contains the string [row,col]
        # return [-1,-1] if doesn't exists
        conditions = [col1 >= len(self.data[0]), col1 < 0, col2 >= len(self.data[0]), col2 < 0, row1 >= len(self.data), row1 < 0, row2 >= len(self.data), row2 < 0 ]
        if any(conditions) is True:
            return False
        for row in range(col1, col2 + 1):
            for col in range(row1, row2 + 1):
                out = self.get_cell(row, col)
                if out == str_to_search:
                    return [row, col]
        return [-1, -1]

    def add_row(self, row1):
        # add a row after row1
        # first add a new blank row
        conditions = [row1 >= len(self.data), row1 < 0]
        if any(conditions) is True:
            return False
        size = len(self.data)
        row_to_add = [""] * len(self.data[0])
        self.read_col_row.append(threading.Semaphore(100))
        self.write_col_row.append(threading.Semaphore())
        self.readCount.append(0)

        self.data.append(row_to_add)

        # exchange between 2 rows until you get to the given row "Piapua"
        for i in range(size, row1 + 1, -1):
            self.exchange_rows(i, i - 1)
        return True

    def add_col(self, col1):
        # add a col after col1
        conditions = [col1 >= len(self.data[0]), col1 < 0]
        if any(conditions) is True:
            return False
        size = len(self.data[0]) - 1
        for row in self.data:
            row.append("")
        self.read_col_row.append(threading.Semaphore())
        self.write_col_row.append(threading.Semaphore())
        self.readCount.append(0)
        # exchange between 2 cols until you get to the given row "piapua"
        for i in range(size, col1 + 1, -1):
            self.exchange_cols(i, i - 1)
        return True


    def save(self, f_name):
        # lock entire sheet
        # once we acquire the sheet do:
        f_name = f_name
        lines = []
        save = "Saved at " + datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S") + "\n"
        lines.append(save)
        lines.append(str(len(self.data)) + ', ' + str(len(self.data[0])) + "\n")
        # append all values that are not none
        for i in range(len(self.data)):
            for j in range(len(self.data[i])):
                if self.data[i][j]:
                    lines.append(str(i) + ', ' + str(j) + ', "' + self.data[i][j] + '"\n')
        with open(f_name, 'w') as file:
            file.writelines(lines)
        # once we are done, release sheet:
        return True

    def load(self, f_name):
        # lock write
        # once we acquire the sheet do:
        with open(f_name, 'r') as file:
            file.readline()  # date line
            size = file.readline()[:-1].split(", ")
            self.data = [["" for i in range(int(size[1]))] for j in range(int(size[0]))]  # initiate new matrix
            cells = file.readlines()
            for cell in cells:
                cell = cell[:-2].split(', ')
                self.data[int(cell[0])][int(cell[1])] = cell[2][1:]
        # once we are done release sheet
        nRows = len(self.data)
        nCols = len(self.data[0])
        n = nRows if nRows >= nCols else nCols
        self.read_col_row = [threading.Semaphore(100) for _ in range(n + 1)]
        self.write_col_row = [threading.Semaphore(1) for _ in range(n + 1)]
        self.readCount = [0 for _ in range(n + 1)]
        return SharableSpreadSheet


    def show(self):

        def popkeys(dictionary, *keys):
            """Pop keys from dictionary.  Return values in list"""
            return [dictionary.pop(key) if key in dictionary else None for key in keys]

        class EntryCell(tk.Entry):
            """tk.Entry widget with a StringVar"""

            def __init__(self, *args, **kvargs):
                self.var = kvargs.get('textvariable', False)
                if not self.var:
                    self.var = tk.StringVar()
                    kvargs['textvariable'] = self.var
                super().__init__(*args, **kvargs)

            def get(self):
                """Return text value"""
                return self.var.get()

            def set(self, value):
                """Set text value"""
                self.var.set(value)

        class Table(tk.Frame):
            """2D matrix of Entry widgets"""

            def __init__(self, parent, rows=None, columns=None, data=None, **kwargs):
                super().__init__(parent)
                if data is not None:
                    rows = len(data)
                    columns = len(data[0])
                elif not rows and columns:
                    raise TypeError('__init__() missing required rows and columns or data argument')
                self.rows = rows
                self.columns = columns
                self.cells = []
                for row in range(rows):
                    for col in range(columns):
                        self.cells.append(EntryCell(self))
                        self.cells[-1].grid(row=row, column=col)
                        if data:
                            self.cells[-1].set(data[row][col])
                self.configure(**kwargs)

            def configure(self, **kwargs):
                """Set configure options.  Adds fg, font, justify and columnwidth
                to Frame's config options
                """
                row, col, bg, fg, font, justify, width = \
                    popkeys(kwargs, 'row', 'column', 'bg', 'fg', 'font', 'justify', 'columnwidth')

                for cell in self.cell(row, col):
                    if bg is not None: cell.configure(bg=bg)
                    if fg is not None: cell.configure(fg=fg)
                    if font is not None: cell.configure(font=font)
                    if justify is not None: cell.configure(justify=justify)
                    if width is not None: cell.configure(width=width)
                if bg is not None: kwargs['bg'] = bg
                if kwargs:
                    super().configure(**kwargs)

            def cell(self, row=None, column=None):
                """Get cell(s).  To get all cells in a row, leave out column.
                To get all cells in a column, leave out row.  To get all cells
                leave out row and column.
                """
                if row is None and column is None:
                    return self.cells
                elif row is None:
                    return [self.cell(row, column) for row in range(self.rows)]
                elif column is None:
                    return [self.cell(row, column) for column in range(self.columns)]
                return self.cells[row * self.columns + column]

        # df = pd.read_csv( "amit.csv" )
        # A = df.values.tolist()
        table_values = self.data

        font = ('Segoe UI', 6, 'bold')
        font2 = ('Segoe UI', 20, 'bold')
        label = "Amit's SharableSpreadSheet"

        root = tk.Tk()
        root.title("Amit's SharableSpreadSheet")
        tk.Label(root, text=label, bg='light blue', font=font2).grid(row=0, column=0, )

        table = Table(root, data=table_values, font=font, bg='light yellow', fg='dark green', bd=5, columnwidth=5,
                      justify=tk.CENTER)
        table.configure(column=0, fg='dark green', justify=tk.CENTER, width=0.2)
        table.configure(column=1, justify=tk.CENTER, width=0.2)
        table.configure(row=0, fg='dark green', justify=tk.CENTER, width=0.1)
        table.grid(row=1, column=0, padx=5, pady=5)

        root.mainloop()

        return True




def spread_sheet_tester(nUsers, nTasks, spreadsheet):
    # test the spreadsheet with random operations and nUsers threads.
    tasks_dict = {1: spreadsheet.get_cell, 2: spreadsheet.set_cell, 3: spreadsheet.search_string,
                  4: spreadsheet.exchange_rows, 5: spreadsheet.exchange_cols, 6: spreadsheet.search_in_row,
                  7: spreadsheet.search_in_col, 8: spreadsheet.search_in_range, 9: spreadsheet.add_row,
                  10: spreadsheet.add_col}

    with concurrent.futures.ThreadPoolExecutor(nUsers) as executor:
        for i in range(nTasks - 1):
            r = random.randint(1, 8)
            print(f"number of task is {r}")
            NumOfCols = len(spreadsheet.data[0])-1   # num cols
            NumOfRows = len(spreadsheet.data)-1  # num rows
            my_task = tasks_dict[r]
            num_row = random.randint(0, 45)
            num_row2 = random.randint(0, 45)
            while num_row == num_row2:
                num_row2 = random.randint(0, NumOfRows)
            num_col = random.randint(0, NumOfCols)
            num_col2 = random.randint(0, NumOfCols)
            while num_col == num_col2:
                num_col2 = random.randint(0, NumOfCols)

            str_lst = ["amitbaras", "harden", "kd", "russel", "lilard","amitfinkman", "deni", "casspi", "kobe",
                       "air_jordan", "shak", "AD", "labron"]
            j = random.randint(1, len(str_lst) - 1)
            my_str = str_lst[j]

            if r == 1: print(executor.submit(my_task, num_row, num_col).result())
            if r == 2: print(executor.submit(my_task, num_row, num_col, my_str).result())
            if r == 3: print(executor.submit(my_task, my_str).result())
            if r == 4: print(executor.submit(my_task, num_row, num_row2).result())
            if r == 5: print(executor.submit(my_task, num_col2, num_col).result())
            if r == 6: print(executor.submit(my_task, num_row, my_str).result())
            if r == 7: print(executor.submit(my_task, num_col, my_str).result())
            if r == 8: print(executor.submit(my_task, num_row, num_row2, num_col, num_col2, my_str).result())
            if r == 9: print(executor.submit(my_task, num_row).result())
            if r == 10: print(executor.submit(my_task, num_col).result())
    # print(spreadsheet.data)
    return spreadsheet


def external_test(n_rows, n_cols, n_users, n_tasks):
    test_spread_sheet = SharableSpreadSheet(n_rows, n_cols)
    test_spread_sheet = spread_sheet_tester(n_users, n_tasks, test_spread_sheet)
    test_spread_sheet.show()
    test_spread_sheet.save('external_test_saved.dat')


if __name__ == '__main__':
    if len(sys.argv) == 5:
        external_test(n_rows=sys.argv[1], n_cols=sys.argv[2], n_users=sys.argv[3], n_tasks=sys.argv[4])
    else:
        # Internal test example (you can change it to check yourself)
        # create, test and save SharableSpreadSheet
        ss = SharableSpreadSheet(40,40)
        ss = spread_sheet_tester(100, 200, ss)

        ss.show()
        ss.save('saved.dat')
        # load, test and save SharableSpreadSheet
        load_ss = SharableSpreadSheet(10, 10)
        load_ss.load('saved.dat')
        load_ss = spread_sheet_tester(20, 10, load_ss)
        load_ss.show()
