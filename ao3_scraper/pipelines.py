# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from datetime import datetime, timezone

from sqlalchemy import text

from ao3_scraper.items import FandomCategoryItem, FandomItem
from database.connection import DatabaseEngine


class FandomsPipeline:

    def open_spider(self, spider):
        self.db_engine = DatabaseEngine()
        self.connection = self.db_engine.connect()
        self._create_tables()

    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        """Pass item to the right processing function"""
        if isinstance(item, FandomCategoryItem):
            self.process_fandom_category(item, spider)
        if isinstance(item, FandomItem):
            self.process_fandom(item, spider)

    def process_fandom_category(self, item, spider):
        now = datetime.now(timezone.utc)
        name = item["name"]
        link = item["link"]
        try:
            self.connection.execute(
                text(
                    """
                    INSERT INTO ao3_scraper.fandom_categories (name, link, created_at, updated_at, _last_crawled_at)
                    VALUES (:name, :link, :now, :now, :now)
                    ON CONFLICT (link) DO UPDATE
                    SET _last_crawled_at = :now
                    """
                ),
                {"name": name, "link": link, "now": now},
            )
            self.connection.commit()
        except:
            spider.logger.error(
                f"Error processing fandom category: {name}", exc_info=True
            )

    def process_fandom(self, item, spider):
        now = datetime.now(timezone.utc)
        name = item["name"]
        link = item["link"]
        fandom_category_link = item["fandom_category_link"]
        try:
            self.connection.execute(
                text(
                    """
                    INSERT INTO ao3_scraper.fandoms (name, link, fandom_category_link, created_at, updated_at, _last_crawled_at)
                    VALUES (:name, :link, :fandom_category_link, :now, :now, :now)
                    ON CONFLICT (link) DO UPDATE
                    SET _last_crawled_at = :now
                    """
                ),
                {
                    "name": name,
                    "link": link,
                    "fandom_category_link": fandom_category_link,
                    "now": now,
                },
            )
            self.connection.commit()
        except:
            spider.logger.error(f"Error processing fandom: {name}", exc_info=True)

    def _create_tables(self):
        self.connection.execute(
            text(
                """
                CREATE SCHEMA IF NOT EXISTS ao3_scraper;

                CREATE TABLE IF NOT EXISTS ao3_scraper.fandom_categories (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    link TEXT NOT NULL UNIQUE,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    _last_crawled_at TIMESTAMP WITH TIME ZONE NOT NULL
                );

                CREATE TABLE IF NOT EXISTS ao3_scraper.fandoms (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    link TEXT NOT NULL UNIQUE,
                    fandom_category_link TEXT NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    _last_crawled_at TIMESTAMP WITH TIME ZONE NOT NULL,

                    CONSTRAINT fk_fandom_fandom_categories
                        FOREIGN KEY (fandom_category_link)
                        REFERENCES ao3_scraper.fandom_categories (link)
                );
                """
            )
        )
        self.connection.commit()


class WorksPipeline:

    def open_spider(self, spider):
        self.db_engine = DatabaseEngine()
        self.connection = self.db_engine.connect()
        self._create_tables()

    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        now = datetime.now(timezone.utc)
        try:
            self.connection.execute(
                text(
                    """
                    INSERT INTO ao3_scraper.works (
                        work_id, title, link, author_name, author_link, fandoms, rating, warnings, category, iswip, work_last_update,
                        optional_tags, series_name, series_link, language, words, chapters, comments, kudos, bookmarks, hits,
                        created_at, updated_at, _last_crawled_at
                    )
                    VALUES (
                        :work_id, :title, :link, :author_name, :author_link, :fandoms, :rating, :warnings, :category, :iswip, :work_last_update,
                        :optional_tags, :series_name, :series_link, :language, :words, :chapters, :comments, :kudos, :bookmarks, :hits,
                        :now, :now, :now
                    )
                    ON CONFLICT (work_id) DO UPDATE
                    SET 
                        title = EXCLUDED.title,
                        link = EXCLUDED.link,
                        author_name = EXCLUDED.author_name,
                        author_link = EXCLUDED.author_link,
                        fandoms = EXCLUDED.fandoms,
                        rating = EXCLUDED.rating,
                        warnings = EXCLUDED.warnings,
                        category = EXCLUDED.category,
                        iswip = EXCLUDED.iswip,
                        work_last_update = EXCLUDED.work_last_update,
                        optional_tags = EXCLUDED.optional_tags,
                        series_name = EXCLUDED.series_name,
                        series_link = EXCLUDED.series_link,
                        language = EXCLUDED.language,
                        words = EXCLUDED.words,
                        chapters = EXCLUDED.chapters,
                        comments = EXCLUDED.comments,
                        kudos = EXCLUDED.kudos,
                        bookmarks = EXCLUDED.bookmarks,
                        hits = EXCLUDED.hits,
                        updated_at = :now,
                        _last_crawled_at = :now
                    """
                ),
                dict(item, now=now),
            )
            self.connection.commit()
        except:
            self.connection.rollback()
            spider.logger.error(
                f"Error processing work: {item['title']}", exc_info=True
            )

    def _create_tables(self):
        self.connection.execute(
            text(
                """
                CREATE SCHEMA IF NOT EXISTS ao3_scraper;

                CREATE TABLE IF NOT EXISTS ao3_scraper.works (
                    id SERIAL PRIMARY KEY,
                    work_id INTEGER NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    link TEXT NOT NULL,
                    author_name TEXT,
                    author_link TEXT,
                    fandoms JSONB NOT NULL,
                    rating TEXT NOT NULL,
                    warnings TEXT NOT NULL,
                    category TEXT NOT NULL,
                    iswip TEXT NOT NULL,
                    work_last_update TIMESTAMP WITH TIME ZONE NOT NULL,
                    optional_tags JSONB,
                    series_name TEXT,
                    series_link TEXT,
                    language TEXT NOT NULL,
                    words INTEGER NOT NULL,
                    chapters INTEGER NOT NULL,
                    comments INTEGER NOT NULL,
                    kudos INTEGER NOT NULL,
                    bookmarks INTEGER NOT NULL,
                    hits INTEGER NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    _last_crawled_at TIMESTAMP WITH TIME ZONE NOT NULL
                );
                """
            )
        )
        self.connection.commit()


class WorkContentPipeline:

    def open_spider(self, spider):
        self.db_engine = DatabaseEngine()
        self.connection = self.db_engine.connect()
        self._create_tables()

    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        now = datetime.now(timezone.utc)
        try:
            self.connection.execute(
                text(
                    """
                    UPDATE ao3_scraper.works
                    SET 
                        download_links = :download_links,
                        work_published_at = :published_at,
                        work_content_html = :work_content_html,
                        _work_content_html_scraped_at = :now
                    WHERE link = :work_link
                    """
                ),
                dict(item, now=now),
            )
            self.connection.commit()
        except:
            self.connection.rollback()
            spider.logger.error(
                f"Error processing work content: {item['work_id']}", exc_info=True
            )

    def _create_tables(self):
        """Update existing ao3_scraper.works table to include new columns"""
        self.connection.execute(
            text(
                """
                ALTER TABLE ao3_scraper.works
                ADD COLUMN IF NOT EXISTS download_links JSONB,
                ADD COLUMN IF NOT EXISTS work_published_at TIMESTAMP WITH TIME ZONE,
                ADD COLUMN IF NOT EXISTS work_content_html TEXT,
                ADD COLUMN IF NOT EXISTS _work_content_html_scraped_at TIMESTAMP WITH TIME ZONE;
                """
            )
        )
        self.connection.commit()
