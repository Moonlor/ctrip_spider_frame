#coding:utf-8
import random

class CrawlerList:
    def __init__(self):
        self.noCrawlerList = []
    def add(self, x):
        self.noCrawlerList.append(x)
    def len(self):
        return len(self.noCrawlerList)
    def delete(self):
        random.shuffle(self.noCrawlerList)
        return self.noCrawlerList.pop()
