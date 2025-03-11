from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from .spiders.works_search import WorksSpider
from .spiders.fandoms import FandomsSpider
from .spiders.work_content import WorkContentSpider

process = CrawlerProcess(get_project_settings())

process.crawl(WorkContentSpider)
process.start()  # the script will block here until the crawling is finished
