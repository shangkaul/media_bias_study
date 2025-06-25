import scrapy
from scrapy.spiders import SitemapSpider
from datetime import datetime
import re
from urllib.parse import urlparse
from typing import Dict, Set
import os

class IndiaSpider(SitemapSpider):
    name = "india_spider"
    allowed_domains = ['india.com']
    
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
        'LOG_LEVEL': 'INFO',
        'LOG_FILE': os.path.join(LOG_DIR, f'india_spider_{date}.log'),
        'LOG_FORMAT': '%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        'FEEDS': {
            os.path.join(DATA_DIR, f'india_articles_{date}.json'): {
                'format': 'json',
                'encoding': 'utf8',
                'store_empty': False,
                'indent': 2
            }
        }
    }

    sitemap_rules = [
        ('/news/', 'parse_article')
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
        'title_path': '//section[contains(@class, "article-details")]/h1[1]/text()',
        'description_path': '//h2[contains(@class, "slug")]//text()',
        'date_path': '//div[contains(@class, "date-share-social")]//p[contains(@class, "date")]/text()',
        'author_path': '//div[contains(@class, "published-by")]//p[contains(@class, "by-name")]//text()',
        'text_path': '//div[contains(@class, "date-share-social")]/following-sibling::*//text()',
        'image_path': '//section[contains(@class, "article-details")]//img[@data-hero and not(contains(@src, ".svg"))]/@src | //section[contains(@class, "article-details")]//img[@data-hero and not(contains(@data-src, ".svg"))]/@data-src',
        'caption_path': '//section[contains(@class, "article-details")]//figcaption[@class="figCap"]/text()'

    }

    def create_start_urls(self):
        # Generate sitemap urls for 7th October 2023 - 7th October 2024
        urls = []

        base = 'https://www.india.com/sitemap' 

        dates = {
            2023 : ['october', 'november', 'december'],
            2024 : ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october','november', 'december'],
            2025 : ['january', 'february', 'march', 'april', 'may']
        }

        for year, months in dates.items():
            for month in months:
                search_url = f'{base}-{month}-{year}.xml'
                urls.append(search_url)

        return urls

    def __init__(self, *args, **kwargs):
        super(IndiaSpider, self).__init__(*args, **kwargs)
        self.sitemap_urls = self.create_start_urls()
        self.keyword_patterns = self._compile_keyword_patterns()
        self.visited_pages = set()  # Track visited pages
        self.stats = {
            'pages_crawled': 0,
            'articles_found': 0,
            'keyword_matches': {},
            'start_time': datetime.now()
        }
    
    def parse_article(self, response):
        """Parse individual article pages"""
        self.stats['pages_crawled'] += 1
        self.logger.info(f"Parsing article: {response.url}")

        if response.url not in self.visited_pages:
            self.visited_pages.add(response.url)
        
            title = ' '.join(response.xpath(self.SITE_CONFIG['title_path']).getall()).strip()
            description = ' '.join(response.xpath(self.SITE_CONFIG['description_path']).getall()).strip()
            text = ' '.join(response.xpath(self.SITE_CONFIG['text_path']).getall()).strip()
            raw_dates = response.xpath('//div[contains(@class, "date-share-social")]//p[contains(@class, "date")]/text()').getall()
            date = next((d.strip() for d in raw_dates if d.strip()), None)
            authors = response.xpath(self.SITE_CONFIG['author_path']).getall()
            images = response.xpath(self.SITE_CONFIG['image_path']).getall()
            # captions = response.xpath(self.SITE_CONFIG['caption_path']).getall()
            captions = [c.strip() for c in response.xpath(self.SITE_CONFIG['caption_path']).getall() if c.strip()]

            if title:
                self.logger.info(f"Found article with title: {title}")

            full_text = f"{title} {description} {text}"
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
                    'description' : description,
                    'text': text,
                    'url': response.url,
                    'source_domain': 'india.com',
                    'date_published': date,
                    'authors': authors,
                    'keywords': list(matches),
                    'matched_keywords': list(matches),
                    'images': images,
                    'captions': captions
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