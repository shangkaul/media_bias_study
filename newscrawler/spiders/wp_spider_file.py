import scrapy
from scrapy.spiders import SitemapSpider
from datetime import datetime
import re
from urllib.parse import urlparse
from typing import Dict, Set
import os

class WPSpider(SitemapSpider):
    name = "wp_spider"
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
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        'DOWNLOAD_DELAY': 5,
        'COOKIES_ENABLED': True,
        'HTTPCACHE_ENABLED': False,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429, 404, 403],
        'LOG_LEVEL': 'DEBUG', #TODO: Change this back to info
        'LOG_FILE': os.path.join(LOG_DIR, f'wp_spider_{date}.log'),
        'LOG_FORMAT': '%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        'FEEDS': {
            os.path.join(DATA_DIR, f'wp_articles_{date}.json'): {
                'format': 'json',
                'encoding': 'utf8',
                'store_empty': False,
                'indent': 2
            }
        },
        'DEFAULT_REQUEST_HEADERS': {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        }
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

    def create_start_urls(self):
        # Generate sitemap urls for 7th October 2023 - 7th October 2024
        urls = []

        urls.append('file:///mnt/raid1/UG4s/AY2425/odutta/dissertation-newscrawler/newscrawler/wpsitemaps/sitemap-2023-12.xml')
        urls.append('file:///mnt/raid1/UG4s/AY2425/odutta/dissertation-newscrawler/newscrawler/wpsitemaps/sitemap-2023-11.xml')
        urls.append('file:///mnt/raid1/UG4s/AY2425/odutta/dissertation-newscrawler/newscrawler/wpsitemaps/sitemap-2023-10.xml')

        urls.append('file:///mnt/raid1/UG4s/AY2425/odutta/dissertation-newscrawler/newscrawler/wpsitemaps/sitemap-2024-10.xml')
        urls.append('file:///mnt/raid1/UG4s/AY2425/odutta/dissertation-newscrawler/newscrawler/wpsitemaps/sitemap-2024-09.xml')
        urls.append('file:///mnt/raid1/UG4s/AY2425/odutta/dissertation-newscrawler/newscrawler/wpsitemaps/sitemap-2024-08.xml')
        urls.append('file:///mnt/raid1/UG4s/AY2425/odutta/dissertation-newscrawler/newscrawler/wpsitemaps/sitemap-2024-07.xml')
        urls.append('file:///mnt/raid1/UG4s/AY2425/odutta/dissertation-newscrawler/newscrawler/wpsitemaps/sitemap-2024-06.xml')
        urls.append('file:///mnt/raid1/UG4s/AY2425/odutta/dissertation-newscrawler/newscrawler/wpsitemaps/sitemap-2024-05.xml')
        urls.append('file:///mnt/raid1/UG4s/AY2425/odutta/dissertation-newscrawler/newscrawler/wpsitemaps/sitemap-2024-04.xml')
        urls.append('file:///mnt/raid1/UG4s/AY2425/odutta/dissertation-newscrawler/newscrawler/wpsitemaps/sitemap-2024-03.xml')
        urls.append('file:///mnt/raid1/UG4s/AY2425/odutta/dissertation-newscrawler/newscrawler/wpsitemaps/sitemap-2024-02.xml')
        urls.append('file:///mnt/raid1/UG4s/AY2425/odutta/dissertation-newscrawler/newscrawler/wpsitemaps/sitemap-2024-01.xml')

        return urls

    def __init__(self, *args, **kwargs):
        self.sitemap_rules = [
            (re.compile(pat), callback) 
            for pat, callback in [
                ('politics', 'parse_article'), 
                ('national', 'parse_article'),
                ('donald-trump', 'parse_article'),
                ('israel-hamas-war', 'parse_article'),
                ('world', 'parse_article'),
                ('world-feed', 'parse_article'),
                ('elon-musk', 'parse_article'),
                ('us-policy', 'parse_article'),
                ('kamala-harris', 'parse_article'),
                ('national-security', 'parse_article'),
                ('foreign-policy', 'parse_article'),
                ('religion', 'parse_article'),
                ('military', 'parse_article')
            ]
        ]
        super(WPSpider, self).__init__(*args, **kwargs)
        self.sitemap_urls = self.create_start_urls()
        self.keyword_patterns = self._compile_keyword_patterns()
        self.visited_pages = set()  # Track visited pages
        self.stats = {
            'pages_crawled': 0,
            'articles_found': 0,
            'keyword_matches': {},
            'start_time': datetime.now()
        }
    
    def start_requests(self):
        
        cookies = {
            "wp_ak_kywrd_ab": "1",
            "wp_geo": "GB|EN|||EEA", 
            "wp_ak_signinv2": "1|20230125",
            "wp_ak_wab": "1|0|1|1|0|1|1|1|0|20230418",
            "wp_ak_v_mab": "0|0|3|1|20240103"
        }

        for url in self.sitemap_urls:
            if url.startswith('file:'):
                yield scrapy.Request(
                    url=url,
                    callback=self._parse_sitemap,
                    dont_filter=True
                )
            else:
                yield scrapy.Request(
                    url=url,
                    cookies=cookies,
                    callback=self._parse_sitemap,
                    dont_filter=True,
                    meta={'handle_httpstatus_list': [403, 404, 429, 500, 502, 503]}
                )

    def _parse_sitemap(self, response):
        """Override _parse_sitemap to add cookies to article requests"""
        cookies = {
            "wp_ak_kywrd_ab": "1",
            "wp_geo": "GB|EN|||EEA",
            "wp_ak_signinv2": "1|20230125",
            "wp_ak_wab": "1|0|1|1|0|1|1|1|0|20230418",
            "wp_ak_v_mab": "0|0|3|1|20240103"
        }

        
        if response.url.endswith('.xml'):
            urls = response.xpath('//xmlns:loc/text()', 
                                namespaces={'xmlns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}).getall()
            
            for url in urls:
                if any(pat.search(url) for pat, _ in self.sitemap_rules):
                    yield scrapy.Request(
                        url=url, 
                        callback=self.parse_article,
                        cookies=cookies,
                        meta={'handle_httpstatus_list': [403, 404, 429, 500, 502, 503]},
                        dont_filter=True
                    )

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