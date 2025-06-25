import scrapy
from scrapy.spiders import SitemapSpider
from datetime import datetime
import re
from urllib.parse import urlparse
from typing import Dict, Set
import os

class CNNSpider(SitemapSpider):
    name = "cnn_spider"
    allowed_domains = ['cnn.com']
    
    date = datetime.now().strftime("%Y%m%d")

    # Set up directory paths
    PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
    LOG_DIR = os.path.join(PROJECT_ROOT, 'logs')
    CACHE_DIR = os.path.join(PROJECT_ROOT, 'cache')

    # Create directories if they don't exist
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(CACHE_DIR, exist_ok=True)

    custom_settings = {
        'ROBOTSTXT_OBEY': True,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 5,
        'DOWNLOAD_DELAY': 3,
        'COOKIES_ENABLED': False,
        'HTTPCACHE_ENABLED':False,
        'REDIRECT_ENABLED': True, 
        'LOG_LEVEL': 'INFO',
        'LOG_FILE': os.path.join(LOG_DIR, f'cnn_spider_{date}.log'),
        'LOG_FORMAT': '%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        'FEEDS': {
            os.path.join(DATA_DIR, f'cnn_articles_{date}.json'): {
                'format': 'json',
                'encoding': 'utf8',
                'store_empty': False,
                'indent': 2
            }
        }
    }

    sitemap_rules = [
        ('/2023/', 'parse_article'),
        ('/2024/', 'parse_article'),
        ('/2025/', 'parse_article')
        
    ]

    KEYWORDS = {
        'primary': [
            'palestine', 'palestinian', 'palestinians', 
            'israel', 'israeli', 'israelis',
            'gaza', 
            'hamas', 
            'idf', 'israeli defence force', 'israeli defence forces', 'israeli defense force', 'israeli defense forces'
        ],
        'date': ['october 7th', '7th october', 'october 7', '7 october', 'oct 7th', '7th oct', 'oct 7', '7 oct']
    }

    SITE_CONFIG = {
        # article fields 
        'title_path': '//h1[contains(@data-editable, "headlineText")]//text()',
        'text_path': '//div[contains(@class, "article__content")]//p//text()',
        'date_path': '//div[contains(@class, "timestamp")]//text()',
        'author_path': '//div[contains(@class, "byline__names")]//span[contains(@class, "byline__name")]/text()',
        'image_main_container': (
            '((//div[contains(@class, "article__content")]'
            '//div['
            '  contains(@class, "image image__hide-placeholder")'
            '  and .//div[contains(@class, "image__metadata")]'
            '  and not(ancestor::*[contains(@class, "related-content")])'
            '])'
            ')'
            '|'
            '((//div[contains(@class, "image__lede")]'
            '//div['
            '  contains(@class, "image image__hide-placeholder")'
            '  and .//div[contains(@class, "image__metadata")]'
            '  and not(ancestor::*[contains(@class, "related-content")])'
            '])'
            ')'
        ),
        'image_src_xpath': './/img/@src',
        'caption_xpath': './/span[@data-editable="metaCaption"]/text()',
    }

    def create_start_urls(self):
        # Generate sitemap urls for 7th October 2023 - 7th October 2024
        urls = []

        bases = ['https://edition.cnn.com/sitemap/article/world', 'https://edition.cnn.com/sitemap/article/politics', 'https://edition.cnn.com/sitemap/article/us']

        dates = {
            2023 : [10, 11, 12],
            2024 : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            2025 : [1, 2, 3, 4, 5]
        }

        for year, months in dates.items():
            for month in months:
                for base in bases:
                    search_url = f'{base}/{year}/{month:02d}.xml'
                    urls.append(search_url)

        return urls

    def __init__(self, *args, **kwargs):
        super(CNNSpider, self).__init__(*args, **kwargs)
        self.sitemap_urls = self.create_start_urls()
        self.keyword_patterns = self._compile_keyword_patterns()
        self.visited_pages = set()  # Track visited pages
        self.stats = {
            'pages_crawled': 0,
            'articles_found': 0,
            'keyword_matches': {},
            'start_time': datetime.now()
        }
    
    # def start_requests(self):
    #     for url in self.sitemap_urls:
    #         yield scrapy.Request(url, callback=self.parse, dont_filter=True, meta={'dont_merge_cookies': True})

    # def parse(self, response):
    #     """Initial parse method - processes listing pages"""
    #     self.logger.info(f"Parsing page: {response.url}")

    #     # Extract all article links
    #     article_links = response.xpath('//url/loc/text()').getall()
    #     self.logger.info(f"Found {len(article_links)} article links on {response.url}")
    
    #     # Process article links
    #     for link in article_links:
    #         url = link

    #         if url not in self.visited_pages:
    #             self.visited_pages.add(url)
    #             self.logger.info(f"Following article link: {url}")
    #             yield scrapy.Request(
    #                 url,
    #                 callback=self.parse_article,
    #                 meta={'dont_redirect': True}
    #             )

    
    def parse_article(self, response):
        """Parse individual article pages"""
        self.stats['pages_crawled'] += 1
        self.logger.info(f"Parsing article: {response.url}")

        if response.url not in self.visited_pages:
            self.visited_pages.add(response.url)
        
            title = ' '.join(response.xpath(self.SITE_CONFIG['title_path']).getall()).strip()
            text = ' '.join(response.xpath(self.SITE_CONFIG['text_path']).getall()).strip()
            date = response.xpath(self.SITE_CONFIG['date_path']).get()
            authors = response.xpath(self.SITE_CONFIG['author_path']).getall()

            images = []
            captions_cleaned = []
            seen = set()

            image_blocks = response.xpath(self.SITE_CONFIG['image_main_container'])

            def is_valid_image(url):
                if not url:
                    return False
                return url.split('?')[0].lower().endswith('.jpg')

            for block in image_blocks:
                img_url = block.xpath(self.SITE_CONFIG['image_src_xpath']).get()
                raw_cap = block.xpath(self.SITE_CONFIG['caption_xpath']).get()

                if is_valid_image(img_url) and img_url not in seen:
                    seen.add(img_url)
                    images.append(img_url.strip())
                    captions_cleaned.append(raw_cap.strip() if raw_cap else "no_caption")


            if title:
                self.logger.info(f"Found article with title: {title}")

            full_text = f"{title} {text}"
            matches = self.find_matches(full_text)

            if matches:
                self.stats['articles_found'] += 1
                self.logger.info(f"Found matching article: {title}")
                self.logger.info(f"Matched keywords: {matches}")

                for match in matches:
                    self.stats['keyword_matches'][match] = \
                        self.stats['keyword_matches'].get(match, 0) + 1

                article = {
                    'title': title,
                    'text': text,
                    'url': response.url,
                    'source_domain': 'cnn.com',
                    'date_published': date,
                    'authors': authors,
                    'keywords': list(matches),
                    'matched_keywords': list(matches),
                    'images': images,
                    'captions': captions_cleaned
                }
                yield article

    def _compile_keyword_patterns(self) -> Dict[str, list]:
        patterns = {}
        for category, keywords in self.KEYWORDS.items():
            patterns[category] = [
                re.compile(rf'\b{re.escape(keyword)}\b', re.IGNORECASE)
                for keyword in keywords
            ]
        return patterns

    def find_matches(self, text: str) -> Set[str]:
        matches = set()
        if not text:
            return matches
            
        for category, patterns in self.keyword_patterns.items():
            for pattern in patterns:
                if pattern.search(text):
                    matches.add(pattern.pattern.lower())
        return matches

    def closed(self, reason):
        elapsed_time = datetime.now() - self.stats['start_time']
        self.logger.info("Spider closed. Final statistics:")
        self.logger.info(f"Reason for closing: {reason}")
        self.logger.info(f"Total run time: {elapsed_time}")
        self.logger.info(f"Total pages crawled: {self.stats['pages_crawled']}")
        self.logger.info(f"Total articles found: {self.stats['articles_found']}")
        for keyword, count in self.stats['keyword_matches'].items():
            self.logger.info(f"Keyword '{keyword}': {count} matches")