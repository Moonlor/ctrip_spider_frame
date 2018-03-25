#coding:utf-8
import random

class CrawlerList:
    def __init__(self):
        self._failed_airline = set()

    def add(self, x):
        self._failed_airline.add(x)

    def len(self):
        return len(self._failed_airline)

    def delete(self):
        return self._failed_airline.pop()
