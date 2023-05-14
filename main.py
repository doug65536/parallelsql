import time

from random import Random
import threading
#import multiprocessing
import mysql.connector

class WorkQueue:
  items = []
  lock = None

  def __init__(self, items):
    self.items = items
    self.lock = threading.Lock()

  def take(self):
    with self.lock:
      if len(self.items) > 0:
        return self.items.pop()
    return None

class WorkDispatcher:
  work: WorkQueue = None

  def __init__(self, work: WorkQueue):
    self.work = work

  def worker(self):
    while True:
      workItem = self.work.take()
      if workItem is None:
        break
      workItem()

def main():
  db = mysql.connector.connect(user='stress', 
    password='g00db0y', database='stress')
  cursor = db.cursor()
  st = time.time()
  clearCustomers(cursor)
  sz = 10000
  for _ in range(sz):
    insertRandomCustomer(cursor)
    db.commit()
  en = time.time()
  el = en - st
  print(sz, 'in', el, 'sec')


def clearCustomers(cursor):
  cursor.execute('delete from customer where 1')

def insertRandomCustomer(cursor):
  customer = {
    'email': randomString(20),
    'lname': randomString(20),
    'fname': randomString(20),
    'addr': randomString(20),
    'city': randomString(20),
    'prov': randomString(20),
    'postal': randomString(20)
  }
  query = """
    INSERT INTO customer SET {sets}""".format(sets=','
      .join([key + '=%(' + key + ')s' for key in customer.keys()])
    )
  cursor.execute(query, customer)
  

def randomString(sz):
  rand = Random()
  return ''.join([chr(rand.randint(65, 65+25)) for _ in range(sz)])

if __name__ == '__main__':
  main()