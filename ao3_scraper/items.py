# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import json
from datetime import datetime, timezone

from scrapy.item import Item, Field
from itemloaders.processors import MapCompose, TakeFirst


def str_to_int(value):
    if not value:
        return 0
    return int(value.replace(",", ""))


def process_chapters(value):
    count = value.split("/")[0]
    if not count:
        return 0
    return str_to_int(count)


def process_work_last_update(value):
    dt = datetime.strptime(value, "%d %b %Y")
    dt = dt.replace(tzinfo=timezone.utc)
    return dt


def process_work_published_at(value):
    dt = datetime.strptime(value, "%Y-%m-%d")
    dt = dt.replace(tzinfo=timezone.utc)
    return dt


def to_json(value):
    return json.dumps(value)


class FandomCategoryItem(Item):
    name = Field()
    link = Field()


class FandomItem(Item):
    name = Field()
    link = Field()
    fandom_category_link = Field()


class WorkItem(Item):
    work_id = Field()
    title = Field()
    link = Field()
    author_name = Field()
    author_link = Field()
    fandoms = Field()
    rating = Field()
    warnings = Field()
    category = Field()
    iswip = Field()
    work_last_update = Field(input_processor=MapCompose(process_work_last_update))
    optional_tags = Field()
    series_name = Field(default=None)
    series_link = Field(default=None)
    language = Field()
    words = Field(input_processor=MapCompose(str_to_int))
    chapters = Field(input_processor=MapCompose(process_chapters))
    comments = Field(input_processor=MapCompose(str_to_int))
    kudos = Field(input_processor=MapCompose(str_to_int))
    bookmarks = Field(input_processor=MapCompose(str_to_int))
    hits = Field(input_processor=MapCompose(str_to_int))


class WorkContentItem(Item):
    work_link = Field(output_processor=TakeFirst())

    # these values are only availble after visiting the work page and not on the search page
    download_links = Field(output_processor=TakeFirst())
    published_at = Field(
        input_processor=MapCompose(process_work_published_at),
        output_processor=TakeFirst(),
    )
    work_content_html = Field(output_processor=TakeFirst())
