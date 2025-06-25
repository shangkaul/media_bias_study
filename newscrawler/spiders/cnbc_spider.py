import scrapy
from datetime import datetime
import re
from urllib.parse import urlparse
from typing import Dict, Set
import os
import random

class CNBCSpider(scrapy.Spider):
    name = "cnbc_spider"
    allowed_domains = ['cnbc.com']
    
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
        'CONCURRENT_REQUESTS_PER_DOMAIN': 3,
        'DOWNLOAD_DELAY': 3,
        'COOKIES_ENABLED': True,
        'HTTPCACHE_ENABLED': False,
        'REDIRECT_ENABLED': False, 
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408, 429, 404, 403],
        'LOG_LEVEL': 'DEBUG',
        'LOG_FILE': os.path.join(LOG_DIR, f'cnbc_spider_{date}.log'),
        'LOG_FORMAT': '%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        'FEEDS': {
            os.path.join(DATA_DIR, f'cnbc_articles_{date}.json'): {
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
        'title_path': '//h1[contains(@class, "ArticleHeader-headline")]//text()',
        'key_points_path' : '//div[contains(@class, "RenderKeyPoints-list")]//li//text()',
        'text_path': '//div[contains(@class, "ArticleBody-articleBody")]//text()',
        'date_path': '//time[contains(@itemprop, "datePublished")]/@datetime',
        'author_path': '//a[contains(@class, "Author-authorName")]//text()',

        # article links 
        'article_link_path': '//a[contains(@class, "SiteMapArticleList-link")]/@href'
    }
    
    def create_start_urls(self):
        # Generate sitemap urls for 7th October 2023 - 7th October 2024
        urls = []

        base = 'https://www.cnbc.com/site-map/articles'

        dates = {
            2023 : {
                'October' : [7, 31],
                'November' : [1, 30],
                'December' : [1, 31]
            },
            2024 : {
                'January' : [1, 31], 
                'February' : [1, 29], # 2024 was a leap year
                'March' : [1, 31], 
                'April' : [1, 30], 
                'May' : [1, 31], 
                'June' : [1, 30], 
                'July' : [1, 31], 
                'August' : [1, 31], 
                'September' : [1, 30], 
                'October' : [1, 7]
            }
        }

        for year, months in dates.items():
            for month, (start, end) in months.items():
                for day in range(start, end+1):
                    
                    search_url = f'{base}/{year}/{month}/{day}/'
                    urls.append(search_url)

        return urls

    def __init__(self, *args, **kwargs):
        super(CNBCSpider, self).__init__(*args, **kwargs)
        self.keyword_patterns = self._compile_keyword_patterns()
        self.start_urls = self.create_start_urls()
        self.visited_pages = set()  # Track visited pages
        self.stats = {
            'pages_crawled': 0,
            'articles_found': 0,
            'keyword_matches': {},
            'start_time': datetime.now()
        }

    def start_requests(self):
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/91.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
        ]
        headers = {
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://www.cnbc.com/',
        }

        for url in self.start_urls:
            yield scrapy.Request(url, headers=headers, callback=self.parse)

    def parse(self, response):
        """Initial parse method - processes listing pages"""
        self.logger.info(f"Parsing page: {response.url}")
        
        # Extract all article links
        article_links = response.xpath(self.SITE_CONFIG['article_link_path']).getall()
        self.logger.info(f"Found {len(article_links)} article links on {response.url}")
        
        # Process article links
        for href in article_links:
            url = href
           
            self.logger.info(f"Following article link: {url}")
            yield scrapy.Request(
                url,
                callback=self.parse_article
            )

    def parse_article(self, response):
        """Parse individual article pages"""
        self.stats['pages_crawled'] += 1
        self.logger.info(f"Parsing article: {response.url}")
        
        title = ' '.join(response.xpath(self.SITE_CONFIG['title_path']).getall()).strip()
        key_points = ' '.join(response.xpath(self.SITE_CONFIG['key_points_path']).getall()).strip()
        text = ' '.join(response.xpath(self.SITE_CONFIG['text_path']).getall()).strip()
        date = response.xpath(self.SITE_CONFIG['date_path']).get()
        authors = response.xpath(self.SITE_CONFIG['author_path']).getall()

        if title:
            self.logger.info(f"Found article with title: {title}")

        full_text = f"{title} {key_points} {text}"
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
                'key_points': key_points,
                'text': text,
                'url': response.url,
                'source_domain': 'cnbc.com',
                'date_published': date,
                'authors': authors,
                'keywords': list(matches),
                'matched_keywords': list(matches)
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