import scrapy


class WorksSpider(scrapy.Spider):
    name = "works"

    def start_requests(self):

        pass

    def parse_works(self, response, fandom_name):

        work_links = response.xpath(
            "//ol[@class='work index group']/li//h4[@class='heading']/a[1]"
        )
        for work in work_links:

            work_link = work.xpath("./@href").get()

            yield response.follow(
                url=work_link,
                callback=self.get_work_data,
            )

    def get_work_data(self, response):

        work_id = response.url.split("/")[-1]

        title = response.xpath("//h2[@class='title heading']/text()").get()

        author_link = response.xpath("//a[@rel='author']/@href").get()
        author_name = response.xpath("//a[@rel='author']/text()").get()

        author = {
            "author_link": author_link,
            "author_name": author_name,
        }

        summary = response.xpath("//div[@class='summary module']//text()").getall()
        summary = "\n".join(summary)

        metadata = self._get_work_metadata(response)

        yield {
            "work_id": work_id,
            "title": title,
            "author": author,
            "summary": summary,
            "metadata": metadata,
        }

    def _get_work_metadata(self, response):

        metadata_dict = {}
        metadata_labels = response.xpath("//dl[@class='work meta group']/dd")
        for metadata_item in metadata_labels:

            name = metadata_item.xpath("./@class").get()
            values = []

            if name == "language":
                values.append(
                    {"value": None, "text": metadata_item.xpath("./text()").get()}
                )

            if name == "stats":
                stats = metadata_item.xpath(".//dd")
                for stat in stats:
                    values.append(
                        {
                            "text": stat.xpath("./@class").get(),
                            "value": stat.xpath("./text()").get(),
                        }
                    )

            for item in metadata_item.xpath(".//a"):
                values.append(
                    {
                        "value": item.xpath("./@href").get(),
                        "text": item.xpath("./text()").get(),
                    }
                )

            metadata_dict[name] = values

        return metadata_dict
