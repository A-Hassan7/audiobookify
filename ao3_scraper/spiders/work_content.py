import json

import scrapy
from scrapy.loader import ItemLoader
from sqlalchemy import text

from database.connection import DatabaseEngine
from ao3_scraper.items import WorkContentItem


class WorkContentSpider(scrapy.Spider):
    name = "work_content"

    KUDOS_THRESHOLD = 10000

    custom_settings = {
        "ITEM_PIPELINES": {"ao3_scraper.pipelines.WorkContentPipeline": 100}
    }

    def start_requests(self):

        db_engine = DatabaseEngine()
        connection = db_engine.connect()
        work_links = connection.execute(
            text(
                f"""
                SELECT distinct link
                FROM ao3_scraper.works
                WHERE
                    kudos > {self.KUDOS_THRESHOLD}
                    -- the work has been updated AFTER the content html was scraped
                    -- OR the work html hasn't been scraped at all
                    AND (
                        work_last_update > _work_content_html_scraped_at
                        OR work_content_html IS NULL
                    )
                 """
            )
        )

        cookies = {
            "view_adult": "true",
        }

        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
            "cache-control": "max-age=0",
            "priority": "u=0, i",
            "sec-ch-ua": '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        }

        for link in work_links:

            # sqlclchemy returns a tuple, I just want the first element
            # TODO: sending request here results in 503 errors from ao3. These requests are being blocked because they work in the browser
            # Could also be an issue with redirects. The page gets redirected to /chapters/{some_id}
            link = link[0]
            yield scrapy.FormRequest(
                url=f"https://archiveofourown.org" + link,
                method="GET",
                headers=headers,
                cookies=cookies,
                formdata={"view_adult": "true"},
                callback=self.parse,
                errback=self.handle_error,
                cb_kwargs={"work_link": link},
            )

    def parse(self, response, work_link):

        download_link_values = []
        download_links = response.xpath("//li[@class='download']/ul/li/a")
        for link in download_links:
            href = link.xpath("./@href").get()
            format_ = link.xpath("./text()").get()
            download_link_values.append({"format": format_, "link": href})

        published_at = response.xpath("//dd[@class='published']/text()").get()

        # get html_content
        html_download_link = [
            link for link in download_link_values if link["format"] == "HTML"
        ]
        if not html_download_link:
            self.logger.error(f"HTML download link not found for {work_link}")

        href = html_download_link[0]["link"]
        url = "https://download.archiveofourown.org" + href
        yield scrapy.Request(
            url=url,
            callback=self.parse_html,
            errback=self.handle_error,
            cb_kwargs={
                "work_link": work_link,
                "published_at": published_at,
                "download_link_values": download_link_values,
            },
        )

    def parse_html(self, response, work_link, published_at, download_link_values):

        # need to use an ItemLoader so the MapCompose function is applied
        loader = ItemLoader(item=WorkContentItem())
        loader.add_value("work_link", work_link)
        loader.add_value("published_at", published_at)
        loader.add_value("download_links", json.dumps(download_link_values))
        loader.add_value("work_content_html", response.text)

        # make sure these are loading properly
        # the works output processor is also only returning a single fandom/optional_tags etc. no good
        yield loader.load_item()

    def handle_error(self, failure):

        self.logger.error(repr(failure))
        with open("errors_work_content.txt", "a") as f:
            f.write(failure.request.url)
            f.write("\n")
