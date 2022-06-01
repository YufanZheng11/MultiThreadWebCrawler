from urllib.parse import urlparse
import threading
import httplib2
from bs4 import BeautifulSoup, SoupStrainer
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(threadName)s %(message)s')


class HtmlParser:

    def getUrls(self, url):
        """ Get all urls of given url page """
        http = httplib2.Http()
        status, response = http.request(url)
        urls = []
        for link in BeautifulSoup(response, 'html.parser', parse_only=SoupStrainer('a')):
            if link.has_attr('href'):
                urls.append(link['href'])
        return urls


class Crawler:

    def __init__(self, startUrl, numThreads=4, maxCrawls=16):
        self._htmlParser = HtmlParser()
        self._startUrlHostName = urlparse(startUrl).hostname
        self._queue = [startUrl]
        self._visited = set()
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._numActiveThreads = 0
        self._done = False
        self._numThreads = numThreads
        self._maxCrawls = maxCrawls

    def crawl(self):
        threads = []
        for _ in range(self._numThreads):
            thread = threading.Thread(target=self._crawl)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        return self._visited

    def _crawl(self):
        while True:
            with self._cv:
                self._cv.wait_for(lambda: len(self._queue) > 0 or self._done)
                if self._done:
                    logging.debug(f"Done")
                    break

                currUrl = self._queue.pop(0)
                self._visited.add(currUrl)
                self._numActiveThreads += 1
                logging.debug(f"Crawling {len(self._visited)} {currUrl}")

            nextUrls = self._htmlParser.getUrls(currUrl)

            with self._cv:
                nextUrls = list(set([url for url in nextUrls if
                                     urlparse(url).hostname == self._startUrlHostName and url not in self._visited]))
                self._queue.extend(nextUrls)
                self._numActiveThreads -= 1
                if self._numActiveThreads == 0 and len(self._queue) == 0:
                    self._done = True
                elif len(self._visited) >= self._maxCrawls:
                    self._done = True

                self._cv.notify_all()


if __name__ == '__main__':
    c = Crawler(startUrl='http://www.nytimes.com')
    c.crawl()
