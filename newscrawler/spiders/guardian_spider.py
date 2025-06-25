import scrapy
from datetime import datetime
import re
from urllib.parse import urlparse
from typing import Dict, Set

class GuardianSpider(scrapy.Spider):
    name = "guardian_spider"
    allowed_domains = ['theguardian.com']
    start_urls = [
        'https://www.theguardian.com/world/gaza',
        'https://www.theguardian.com/world/israel',
        'https://www.theguardian.com/world/middleeast'
    ]

    custom_settings = {
        'ROBOTSTXT_OBEY': True,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 4,
        'DOWNLOAD_DELAY': 3,
        'COOKIES_ENABLED': False,
        'LOG_LEVEL': 'DEBUG',
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
        'title_path': '//h1[contains(@class, "dcr-") or contains(@class, "article-headline")]//text()',
        'text_path': '//div[contains(@class, "article-body") or contains(@class, "dcr-")]//p//text()',
        'date_path': '//time/@datetime',
        'author_path': '//a[contains(@rel, "author")]//text()',
        'article_link_path': '//a[contains(@href, "/2023/") or contains(@href, "/2024/")]/@href',
        'pagination_container': '//div[contains(@class, "dcr-stdtpu")]',
        'page_links': './/a[contains(@class, "dcr-1nzqxjn")]/@href'
    }

    def __init__(self, *args, **kwargs):
        super(GuardianSpider, self).__init__(*args, **kwargs)
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
            
            if self.is_relevant_url(url):
                self.logger.info(f"Following article link: {url}")
                yield scrapy.Request(
                    url,
                    callback=self.parse_article,
                    meta={'dont_redirect': True}
                )

        # Handle pagination
        pagination_container = response.xpath(self.SITE_CONFIG['pagination_container'])
        if pagination_container:
            page_links = pagination_container.xpath(self.SITE_CONFIG['page_links']).getall()
            
            for href in page_links:
                next_url = response.urljoin(href)
                if next_url not in self.visited_pages:
                    self.visited_pages.add(next_url)
                    self.logger.info(f"Following pagination link: {next_url}")
                    yield scrapy.Request(
                        next_url,
                        callback=self.parse,
                        meta={'dont_redirect': True}
                    )

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
                'source_domain': 'theguardian.com',
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