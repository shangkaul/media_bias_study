import scrapy
from scrapy.spiders import SitemapSpider
from datetime import datetime
import re
from urllib.parse import urlparse
from typing import Dict, Set
import os

class WashingtonPostSpider(scrapy.Spider):
    name = "washington_post_spider"
    allowed_domains = ['washingtonpost.com']
    
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
        'USER_AGENT' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'CONCURRENT_REQUESTS_PER_DOMAIN': 5,
        'DOWNLOAD_DELAY': 3,
        'COOKIES_ENABLED': True,
        'HTTPCACHE_ENABLED': False,
        'DOWNLOAD_TIMEOUT': 180,
        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': os.path.join(LOG_DIR, f'washington_post_spider_{date}.log'),
        'LOG_FORMAT': '%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        'FEEDS': {
            os.path.join(DATA_DIR, f'washington_post_articles_{date}.json'): {
                'format': 'json',
                'encoding': 'utf8',
                'store_empty': False,
                'indent': 2
            }
        }
    }

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
        'title_path': '//h1[contains(@data-testid, "headline")]/text()',
        'description_path': '//p[contains(@data-testid, "subheadline")]//text()',
        'date_path': '//span[contains(@data-testid, "published-date")]//text()',
        'author_path': '//a[contains(@rel, "author")]//text()',
        'text_path': '//div[contains(@class, "meteredContent")]//text()'
    }

    sitemap_rules = [
        ('politics', 'parse_article'), 
        ('national', 'parse_article'),
        ('donald-trump','parse_article'),
        ('israel-hamas-war', 'parse_article'),
        ('world','parse_article'),
        ('world-feed','parse_article'),
        ('elon-musk','parse_article'),
        ('us-policy','parse_article'),
        ('kamala-harris','parse_article'),
        ('national-security','parse_article'),
        ('foreign-policy','parse_article'),
        ('religion','parse_article'),
        ('military','parse_article')
    ]

    def create_start_urls(self):
        # Generate sitemap urls for 7th October 2023 - 7th October 2024
        urls = []

        base = 'https://www.washingtonpost.com/sitemaps/sitemap' 
        
        dates = {
            2023 : [10, 11, 12],
            2024 : [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        }

        for year, months in dates.items():
            for month in months:
                search_url = f'{base}-{year}-{month:02d}.xml'
                urls.append(search_url)

        urls = ['https://www.washingtonpost.com/sitemaps/sitemap-2023-12.xml']

        return urls

    def __init__(self, *args, **kwargs):
        super(WashingtonPostSpider, self).__init__(*args, **kwargs)
        self.start_urls = self.create_start_urls()
        self.keyword_patterns = self._compile_keyword_patterns()
        self.visited_pages = set()  # Track visited pages
        self.stats = {
            'pages_crawled': 0,
            'articles_found': 0,
            'keyword_matches': {},
            'start_time': datetime.now()
        }
    
    def start_requests(self):
        headers = {"User-Agent":"Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36"}
        cookies = {"wp_ak_kywrd_ab": "1",
        "wp_geo": "GB|EN|||EEA",
        "wp_ak_signinv2": "1|20230125",
        "wp_ak_wab": "1|0|1|1|0|1|1|1|0|20230418",
        "wp_ak_v_mab": "0|0|3|1|20240103"}
        for url in self.start_urls:
            yield scrapy.Request(url=url,headers=headers, cookies=cookies, callback=self.parse_sitemap)

    def parse_sitemap(self, response):
        self.logger.info(f"Parsing sitemap: {response.url}")
        article_urls = response.xpath('//url/loc/text()').getall()
        for article_url in article_urls:
            for keyword, callback in self.sitemap_rules:
                if keyword in article_url:
                    yield scrapy.Request(url=article_url, callback=getattr(self, callback))
                    break
        
    def parse_article(self, response):
        """Parse individual article pages"""
        self.stats['pages_crawled'] += 1
        self.logger.info(f"Parsing article: {response.url}")

        if response.url not in self.visited_pages:
            self.visited_pages.add(response.url)
        
            title = ' '.join(response.xpath(self.SITE_CONFIG['title_path']).getall()).strip()
            description = ' '.join(response.xpath(self.SITE_CONFIG['description_path']).getall()).strip()
            text = ' '.join(response.xpath(self.SITE_CONFIG['text_path']).getall()).strip()
            date = response.xpath(self.SITE_CONFIG['date_path']).get()
            authors = response.xpath(self.SITE_CONFIG['author_path']).getall()

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
                    'source_domain': 'washingtonpost.com',
                    'date_published': date,
                    'authors': authors,
                    'keywords': list(matches),
                    'matched_keywords': list(matches),
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