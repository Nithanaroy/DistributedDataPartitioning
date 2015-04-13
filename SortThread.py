__author__ = 'Nitin Pasumarthy'

from ThreadWithReturnValue import ThreadWithReturnValue
import RatingsDAO


class SortThread(ThreadWithReturnValue):
    def __init__(self, conn, col, order, table):
        ThreadWithReturnValue.__init__(self)
        self.conn = conn
        self.col = col
        self.order = order
        self.table = table

    def run(self):
        return RatingsDAO.get_sorted_rows(self.conn, self.col, self.order, self.table)