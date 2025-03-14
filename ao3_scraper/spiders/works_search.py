import json

import scrapy
from sqlalchemy import text
from itemloaders import ItemLoader
from itemloaders.processors import TakeFirst


from ao3_scraper.items import WorkItem
from database.connection import DatabaseEngine


class WorksSpider(scrapy.Spider):
    """
    Scrape works for every fandom. Currently grabs the first 5000 pages for each fandom sorted by kudos count.

    This grabs the metadata for each work but not the actual work content because the download links don't seem to be on the page.
    The work_content spider grabs the actual content and adds it to the works table in the database.
    """

    name = "works"

    custom_settings = {"ITEM_PIPELINES": {"ao3_scraper.pipelines.WorksPipeline": 100}}

    MIN_KUDOS_COUNT = 100

    def start_requests(self, maximum_kudos=None):
        """
        Uses the search feature on ao3 to get works in descending order of kudos count.
        Once I've hit the 5000 page limit I grab the smallest kudos count and use that as the maximum kudos count for the next page.
        """

        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
            "priority": "u=0, i",
            "referer": "https://archiveofourown.org/works/search?commit=Search&page=5000&work_search%5Bbookmarks_count%5D=&work_search%5Bcharacter_names%5D=&work_search%5Bcomments_count%5D=&work_search%5Bcomplete%5D=&work_search%5Bcreators%5D=&work_search%5Bcrossover%5D=&work_search%5Bfandom_names%5D=&work_search%5Bfreeform_names%5D=&work_search%5Bhits%5D=&work_search%5Bkudos_count%5D=%26lt%3B1298&work_search%5Blanguage_id%5D=&work_search%5Bquery%5D=&work_search%5Brating_ids%5D=&work_search%5Brelationship_names%5D=&work_search%5Brevised_at%5D=&work_search%5Bsingle_chapter%5D=0&work_search%5Bsort_column%5D=kudos_count&work_search%5Bsort_direction%5D=desc&work_search%5Btitle%5D=&work_search%5Bword_count%5D=",
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

        params = {
            "commit": "Search",
            "page": "1",
            "work_search[bookmarks_count]": "",
            "work_search[character_names]": "",
            "work_search[comments_count]": "",
            "work_search[complete]": "",
            "work_search[creators]": "",
            "work_search[crossover]": "",
            "work_search[fandom_names]": "",
            "work_search[freeform_names]": "",
            "work_search[hits]": "",
            "work_search[kudos_count]": f"&lt;{maximum_kudos}" if maximum_kudos else "",
            "work_search[language_id]": "",
            "work_search[query]": "",
            "work_search[rating_ids]": "",
            "work_search[relationship_names]": "",
            "work_search[revised_at]": "",
            "work_search[single_chapter]": "0",
            "work_search[sort_column]": "kudos_count",
            "work_search[sort_direction]": "desc",
            "work_search[title]": "",
            "work_search[word_count]": "",
        }

        headers = {
            "Referer": "https://archiveofourown.org/works/search",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
            "sec-ch-ua": '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
        }

        # create request for 5000 pages
        # this is faster than following the next page link since I can create the requests in bulk here for every page
        for page in range(1, 5001):

            # prevent modifying the original params dict since this messes up other requests running asynchronously
            params_copy = params.copy()
            params_copy["page"] = str(page)
            yield scrapy.FormRequest(
                url="https://archiveofourown.org/works/search",
                method="GET",
                headers=headers,
                formdata=params_copy,
                callback=self.parse_works,
                errback=self.handle_error,
                cb_kwargs={"page": page},
            )

    def parse_works(self, response, page):
        """Get data for each work"""

        # I've likely hit the 5000 page limit
        # reduce the maximum kudos count to get the next page
        if page == 5000:
            self.logger.info("Reached 5000 pages")

            # get the smallest kudos count on the page so I can use for the next search
            kudos_list = response.xpath(
                f"//dl[@class='stats']/dd[@class='kudos']/a/text()"
            ).getall()
            kudos_list = [int(kudos.replace(",", "")) for kudos in kudos_list]
            min_kudos = min(kudos_list)

            # don't want to scrape more works if they have less than 100 kudos
            if min_kudos < self.MIN_KUDOS_COUNT:
                return

            # increase the kudos count so I don't skip works with the same kudos count
            min_kudos += 1
            for item in self.start_requests(maximum_kudos=min_kudos):
                yield item

        work_cards = response.xpath("//ol[@class='work index group']/li")

        for card in work_cards:
            item = WorkItem()

            work_id = card.xpath("./@id").get()
            item["work_id"] = work_id.split("_")[-1]

            header = card.xpath("./div[@class='header module']")

            # work name and author
            title = header.xpath("./*[@class='heading']")
            item["title"] = title.xpath("./a[1]/text()").get()
            item["link"] = title.xpath("./a[1]/@href").get()
            item["author_name"] = title.xpath('./a[@rel="author"]/text()').get()
            item["author_link"] = title.xpath('./a[@rel="author"]/@href').get()

            # list of fandoms the work is associated with
            fandoms_values = []
            fandoms_list = header.xpath("./*[@class='fandoms heading']/a[@class='tag']")
            for fandom in fandoms_list:
                fandom_name = fandom.xpath("./text()").get()
                fandom_link = fandom.xpath("./@href").get()
                fandoms_values.append({"name": fandom_name, "link": fandom_link})
            item["fandoms"] = json.dumps(fandoms_values)

            # things like rating, warning etc. (these are the icons on the top left)
            required_tags = header.xpath("./ul[@class='required-tags']/li/a/span")
            for tag in required_tags:
                tag_name = tag.xpath("./@class").get()
                tag_name = tag_name.split(" ")[-1]
                tag_value = tag.xpath("./@title").get()

                item[tag_name] = tag_value

            # date the work was last updated
            item["work_last_update"] = header.xpath(
                "./p[@class='datetime']/text()"
            ).get()

            # optional tags
            optional_tags_values = []
            optional_tags = card.xpath("./ul[@class='tags commas']/li")
            for tag in optional_tags:
                tag_type = tag.xpath("./@class").get()
                tag_name = tag.xpath(".//a/text()").get()
                tag_link = tag.xpath(".//a/@href").get()
                optional_tags_values.append(
                    {"type": tag_type, "name": tag_name, "link": tag_link}
                )
            item["optional_tags"] = json.dumps(optional_tags_values)

            # series
            series = card.xpath("./ul[@class='series']/li")
            if series:
                series_name = series.xpath(".//text()").getall()
                series_name = "".join(series_name).strip()
                series_link = series.xpath("./a/@href").get()

            item["series_name"] = series_name if series else None
            item["series_link"] = series_link if series else None

            # language
            stat_names = [
                "language",
                "words",
                "chapters",
                "comments",
                "kudos",
                "bookmarks",
                "hits",
            ]
            for stat_name in stat_names:
                stat = card.xpath(f"./dl[@class='stats']/dd[@class='{stat_name}']")
                stat_value = stat.xpath(".//text()").getall()
                stat_value = "".join(stat_value)
                item[stat_name] = stat_value

            # use an item loader so input_processors work
            loader = ItemLoader(item=item)
            # take the last value in the output list
            loader.default_output_processor = lambda x: x[-1] if x else None
            for key, value in item.items():
                loader.add_value(key, value)

            yield loader.load_item()

        # if there is no next page and I haven't reached the 5000 page limit on the search
        # then I've reached the end of the search results
        next_page = response.xpath(
            "//ol[@class='pagination actions']/li[@class='next'][1]/a/@href"
        ).get()
        if not next_page and page < 5000:
            return

    def handle_error(self, failure):

        self.logger.error(repr(failure))
        with open("errors_works.txt", "a") as f:
            f.write(failure.request.url)
            f.write("\n")
