import scrapy

from ao3_scraper.items import FandomCategoryItem, FandomItem


class FandomsSpider(scrapy.Spider):
    name = "fandoms"

    custom_settings = {"ITEM_PIPELINES": {"ao3_scraper.pipelines.FandomsPipeline": 100}}

    def start_requests(self):
        url = "https://archiveofourown.org/media"

        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
            "cache-control": "max-age=0",
            "priority": "u=0, i",
            "referer": "https://archiveofourown.org/users/MsKingBean89/pseuds/MsKingBean89",
            "sec-ch-ua": '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        }

        yield scrapy.Request(
            url=url,
            headers=headers,
            callback=self.parse_fandom_categories,
            errback=self.handle_error,
        )

    def parse_fandom_categories(self, response):
        """Get a list of all the fandom categories on the main page of A03"""

        links = response.xpath(
            "//ul[@class='media fandom index group']//li//h3[@class='heading']//a"
        )

        if not links:
            self.logger.critical("No fandom categories found")

        for link in links:

            href = link.xpath("./@href").get()
            name = link.xpath("./text()").get()

            yield FandomCategoryItem(name=name, link=href)

            # follow link to fandoms within the category
            yield response.follow(
                url=href,
                callback=self.parse_fandoms,
                cb_kwargs={"fandom_category_link": href},
            )

    def parse_fandoms(self, response, fandom_category_link):
        """Get the list of all fandoms within a fandom category"""

        links = response.xpath("//ul[@class='tags index group']//li//a[@class='tag']")

        if not links:
            self.logger.critical(f"No fictions found in fandom {response.url}")

        for link in links:

            href = link.xpath("./@href").get()
            name = link.xpath("./text()").get()

            if not name:
                name = href.split("/")[-2]

            yield FandomItem(
                name=name, link=href, fandom_category_link=fandom_category_link
            )

    def handle_error(self, failure):

        self.logger.error(repr(failure))
        with open("errors_fandoms.txt", "a") as f:
            f.write(failure.request.url)
            f.write("\n")
