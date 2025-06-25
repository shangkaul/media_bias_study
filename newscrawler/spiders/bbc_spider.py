import scrapy
from datetime import datetime
import re
from urllib.parse import urlparse
from typing import Dict, Set
import os

class BBCSpider(scrapy.Spider):
    name = "bbc_spider"
    allowed_domains = ['bbc.com', 'bbc.co.uk']
    start_urls = [
        'https://www.bbc.com/search?q=palestine&edgeauth=eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9.eyJrZXkiOiAiZmFzdGx5LXVyaS10b2tlbi0xIiwiZXhwIjogMTczNjE1MTU4NywibmJmIjogMTczNjE1MTIyNywicmVxdWVzdHVyaSI6ICIlMkZzZWFyY2glM0ZxJTNEcGFsZXN0aW5lIn0.jf3sl42ad2m-dvS69LnhtNFOuJHgTJvsIwM4xnjHYG8', # palestine
        'https://www.bbc.com/search?q=palestinian&edgeauth=eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9.eyJrZXkiOiAiZmFzdGx5LXVyaS10b2tlbi0xIiwiZXhwIjogMTczNjE1MTYzOSwibmJmIjogMTczNjE1MTI3OSwicmVxdWVzdHVyaSI6ICIlMkZzZWFyY2glM0ZxJTNEcGFsZXN0aW5pYW4ifQ.Ap2RCXO2zxlBo-qcF8AiFuP4JNDPGXkrDQBc7dbh0BI', # palestinian
        'https://www.bbc.com/search?q=palestinians&edgeauth=eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9.eyJrZXkiOiAiZmFzdGx5LXVyaS10b2tlbi0xIiwiZXhwIjogMTczNjE1MTY4OCwibmJmIjogMTczNjE1MTMyOCwicmVxdWVzdHVyaSI6ICIlMkZzZWFyY2glM0ZxJTNEcGFsZXN0aW5pYW5zIn0.ht4wpwMImvZyIgkFlz4KMbRmtSLWvyWKtJe6iIzcjdg', # palestinians
        'https://www.bbc.com/search?q=israel&edgeauth=eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9.eyJrZXkiOiAiZmFzdGx5LXVyaS10b2tlbi0xIiwiZXhwIjogMTczNjE1MTczNiwibmJmIjogMTczNjE1MTM3NiwicmVxdWVzdHVyaSI6ICIlMkZzZWFyY2glM0ZxJTNEaXNyYWVsIn0.Gei4Dn5dnuz0EHiNE1U04q1QZH1Kofr93fct3Nkrzg8', # israel
        'https://www.bbc.com/search?q=israeli&edgeauth=eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9.eyJrZXkiOiAiZmFzdGx5LXVyaS10b2tlbi0xIiwiZXhwIjogMTczNjE1MTc1MiwibmJmIjogMTczNjE1MTM5MiwicmVxdWVzdHVyaSI6ICIlMkZzZWFyY2glM0ZxJTNEaXNyYWVsaSJ9.-dvbyK7H-195s2tK2CK3ILs6LdjVD3-aMRVbZm_M0Jo', # israeli
        'https://www.bbc.com/search?q=israelis&edgeauth=eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9.eyJrZXkiOiAiZmFzdGx5LXVyaS10b2tlbi0xIiwiZXhwIjogMTczNjE1MTc4MCwibmJmIjogMTczNjE1MTQyMCwicmVxdWVzdHVyaSI6ICIlMkZzZWFyY2glM0ZxJTNEaXNyYWVsaXMifQ.DtOJDbAhAe4C3_5k3nlvWgZwTTrGbornFy7unNzVbpw', # israelis
        'https://www.bbc.com/search?q=gaza&edgeauth=eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9.eyJrZXkiOiAiZmFzdGx5LXVyaS10b2tlbi0xIiwiZXhwIjogMTczNjE1MTg2NywibmJmIjogMTczNjE1MTUwNywicmVxdWVzdHVyaSI6ICIlMkZzZWFyY2glM0ZxJTNEZ2F6YSJ9.2tL3LjIygrsIC2-14Mz0E1VJ9lWiKEoBTnWY_em0Pd4', # gaza
        'https://www.bbc.com/search?q=hamas&edgeauth=eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9.eyJrZXkiOiAiZmFzdGx5LXVyaS10b2tlbi0xIiwiZXhwIjogMTczNjE1MjY3NSwibmJmIjogMTczNjE1MjMxNSwicmVxdWVzdHVyaSI6ICIlMkZzZWFyY2glM0ZxJTNEaGFtYXMifQ.-J111JfRpfABNDU_GtA6FSqhqWvNsA00gOMLL1fJPp8', # hamas
        'https://www.bbc.com/search?q=idf&edgeauth=eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9.eyJrZXkiOiAiZmFzdGx5LXVyaS10b2tlbi0xIiwiZXhwIjogMTczNjE1MzE1MiwibmJmIjogMTczNjE1Mjc5MiwicmVxdWVzdHVyaSI6ICIlMkZzZWFyY2glM0ZxJTNEaWRmIn0.2mAAgM0OzPTEKqpkccvR_QWmuekA_hJWEXM699tM-Lg', # idf
        'https://www.bbc.com/search?q=israeli+defence+force&edgeauth=eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9.eyJrZXkiOiAiZmFzdGx5LXVyaS10b2tlbi0xIiwiZXhwIjogMTczNjE1MzE3OSwibmJmIjogMTczNjE1MjgxOSwicmVxdWVzdHVyaSI6ICIlMkZzZWFyY2glM0ZxJTNEaXNyYWVsaSUyQmRlZmVuY2UlMkJmb3JjZSJ9.AFXx4LJpuFyJ4VpC6x4WMZBfgDMCBDwIduhR8JxA2pQ', # israeli defence force
        'https://www.bbc.com/search?q=israeli+defence+forces&edgeauth=eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9.eyJrZXkiOiAiZmFzdGx5LXVyaS10b2tlbi0xIiwiZXhwIjogMTczNjE1MzI1NCwibmJmIjogMTczNjE1Mjg5NCwicmVxdWVzdHVyaSI6ICIlMkZzZWFyY2glM0ZxJTNEaXNyYWVsaSUyQmRlZmVuY2UlMkJmb3JjZXMifQ.JIYu4nx7vmmU5SwzfEHAZewjGYcvne-y3ofIzow-xtE', # israeli defence forces
        'https://www.bbc.com/search?q=october+7th&edgeauth=eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9.eyJrZXkiOiAiZmFzdGx5LXVyaS10b2tlbi0xIiwiZXhwIjogMTczNjE1MzI4NSwibmJmIjogMTczNjE1MjkyNSwicmVxdWVzdHVyaSI6ICIlMkZzZWFyY2glM0ZxJTNEb2N0b2JlciUyQjd0aCJ9.rtXMNmM7dyfB76ya4CmKkgsFLOpAfqZe0r2q4F_rvvM', # october 7th
    ]

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
        'CONCURRENT_REQUESTS_PER_DOMAIN': 4,
        'DOWNLOAD_DELAY': 3,
        'COOKIES_ENABLED': False,
        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': os.path.join(LOG_DIR, f'bbc_spider_{date}.log'),
        'LOG_FORMAT': '%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        'FEEDS': {
            os.path.join(DATA_DIR, f'bbc_spider_articles_{date}.json'): {
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
        'title_path': '//h1[contains(@id, "main-heading") or contains(@class, "Heading") or contains(@class, "bWszMR")]//text()',
        'text_path': '//div[contains(@class, "RichTextComponentWrapper") or contains(@data-component, "text-block")]//p//text()',
        'date_path': '//time/@datetime | //time[not(@datetime) and contains(@class, "fkLXLN")]/text()',
        'author_path': '//div[contains(@class, "bpnWmT")]|//div[contains(@class, "TextContributorName")]//text()',
        # article links
        'article_link_path': '//div[@data-testid="newport-card"]//a[contains(@data-testid="internal-link")]/@href',
        # pagination
        'pagination_container': '//div[contains(@data-testid="pagination")]',
        'last_page':'.//button[not(@data-testid) and not(contains(text(), "..."))][last()]//text()'
    }

    def __init__(self, *args, **kwargs):
        super(BBCSpider, self).__init__(*args, **kwargs)
        self.keyword_patterns = self._compile_keyword_patterns()
        self.visited_pages = set()  # Track visited pages
        self.stats = {
            'pages_crawled': 0,
            'articles_found': 0,
            'keyword_matches': {},
            'start_time': datetime.now()
        }

    def parse(self, response):
        """Initial parse method - processes listing pages"""
        self.logger.info(f"Parsing page: {response.url}")
        
        # Extract all article links
        article_links = response.xpath(self.SITE_CONFIG['article_link_path']).getall()
        self.logger.info(f"Found {len(article_links)} article links on {response.url}")
        
        # Process article links
        for href in article_links:
            url = response.urljoin(href)
            self.logger.debug(f"Checking URL: {url}")
            
            
            self.logger.info(f"Following article link: {url}")
            yield scrapy.Request(
                url,
                callback=self.parse_article,
                meta={'dont_redirect': True}
            )

        # Handle pagination
        pagination_container = response.xpath(self.SITE_CONFIG['pagination_container'])
        if pagination_container:
            
            last_page_num = pagination_container.xpath(self.SITE_CONFIG['last_page']).get()
            
            if last_page_num:
                try:
                    last_page_int = int(last_page_num)
                    self.logger.info("Last page number extracted successfully")

                    base = response.url.split("&page=")[0]

                    for n in range(1, last_page_int):
                        next_url = f"{base}&page={n - 1}" # bbc search is indexed from 0
                        if next_url not in self.visited_pages:
                            self.visited_pages.add(next_url)
                            self.logger.info(f"Following pagination link: {next_url}")
                            yield scrapy.Request(
                                next_url,
                                callback=self.parse,
                                meta={'dont_redirect': True}
                            )
                except Exception as e:
                    self.logger.error(f"Error when creating pagination urls: {e}")
                    

    
    def parse_article(self, response):
        """Parse individual article pages"""
        self.stats['pages_crawled'] += 1
        self.logger.info(f"Parsing article: {response.url}")
        
        title = ' '.join(response.xpath(self.SITE_CONFIG['title_path']).getall()).strip()
        text = ' '.join(response.xpath(self.SITE_CONFIG['text_path']).getall()).strip()
        date = response.xpath(self.SITE_CONFIG['date_path']).get()
        authors = response.xpath(self.SITE_CONFIG['author_path']).getall()

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
                'date_published': date,
                'authors': authors,
                'keywords': list(matches),
                'matched_keywords': list(matches)
            }
            yield article

    def is_relevant_url(self, url: str) -> bool:
        """Check if URL is relevant (within date range)"""
        parsed_url = urlparse(url)
        path = parsed_url.path
        
        month_to_num = {
            'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 
            'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
            'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
        }
        
        date_str = re.search(r"/(\d{4})/([a-z]{3})/(\d{1,2})/", path)
        date_relevant = False

        if date_str:
            year, month, day = date_str.groups()
            year = int(year)
            month = month_to_num[month.lower()]
            day = int(day)

            start_date = datetime(2023, 10, 7)
            end_date = datetime(2024, 10, 7)

            if start_date <= datetime(year, month, day) <= end_date:
                date_relevant = True
        
        is_article = any(x in path for x in ['/article/', '/live/', '/2023/', '/2024/'])
        
        ignore_patterns = [
            r'/video/', r'/gallery/', r'/audio/',
            r'/pictures/', r'/commentisfree/'
        ]
        
        should_ignore = any(re.search(pattern, path, re.IGNORECASE) for pattern in ignore_patterns)
        
        return is_article and not should_ignore and date_relevant

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