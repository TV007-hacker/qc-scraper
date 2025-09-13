#!/usr/bin/env python3
"""
Quick Commerce Industry News Scraper
Collects news articles about quick commerce, q-commerce, and ultra-fast delivery
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import logging
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
import re
import os
from typing import List, Dict, Optional
import argparse
from collections import defaultdict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class QuickCommerceNewsScraper:
    def __init__(self, timeframe='7d'):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Timeframe configuration
        self.timeframe = timeframe
        self.timeframe_options = {
            '6h': {'hours': 6, 'description': 'Last 6 hours'},
            '12h': {'hours': 12, 'description': 'Last 12 hours'},
            '24h': {'hours': 24, 'description': 'Last 24 hours'},
            '2d': {'days': 2, 'description': 'Last 2 days'},
            '3d': {'days': 3, 'description': 'Last 3 days'},
            '7d': {'days': 7, 'description': 'Last week'},
            '14d': {'days': 14, 'description': 'Last 2 weeks'},
            '30d': {'days': 30, 'description': 'Last month'},
            '60d': {'days': 60, 'description': 'Last 2 months'},
            '90d': {'days': 90, 'description': 'Last 3 months'},
            'custom': {'days': None, 'description': 'Custom date range'}
        }
        
        self.start_date, self.end_date = self._calculate_timeframe()
        
        # Enhanced quick commerce related keywords for Indian market
        self.keywords = [
            'quick commerce', 'q-commerce', 'quick-commerce', 'qcommerce',
            'blinkit', 'zepto', 'swiggy instamart', 'instamart', 'swiggy instant',
            'amazon now', 'flipkart minutes', 'myntra rapid', 'bigbasket now',
            'dunzo', 'grofers', 'milk basket', 'fresh to home instant',
            'gopuff', 'getir', 'gorillas', 'flink', 'jokr', 'weezy',
            'ultra fast delivery', '10 minute delivery', '15 minute delivery', '30 minute delivery',
            'instant grocery', 'instant delivery', 'rapid delivery', 'express delivery',
            'dark store', 'dark stores', 'micro fulfillment', 'micro-fulfillment',
            'on-demand delivery', 'hyperlocal delivery', 'last mile delivery',
            'grocery delivery', 'food delivery instant', 'medicine delivery instant'
        ]
        
        # Comprehensive Indian news sources with search URLs and RSS feeds
        self.news_sources = {
            # Major Indian Business & Tech News
            'Economic Times': {
                'search_url': 'https://economictimes.indiatimes.com/searchresult.cms?query={}',
                'rss_url': 'https://economictimes.indiatimes.com/rssfeedsdefault.cms',
                'base_url': 'https://economictimes.indiatimes.com'
            },
            'Business Standard': {
                'search_url': 'https://www.business-standard.com/search?q={}',
                'rss_url': 'https://www.business-standard.com/rss/latest.rss',
                'base_url': 'https://www.business-standard.com'
            },
            'Financial Express': {
                'search_url': 'https://www.financialexpress.com/search/?q={}',
                'rss_url': 'https://www.financialexpress.com/feed/',
                'base_url': 'https://www.financialexpress.com'
            },
            'LiveMint': {
                'search_url': 'https://www.livemint.com/Search/Link/Keyword/{}',
                'rss_url': 'https://www.livemint.com/rss/companies',
                'base_url': 'https://www.livemint.com'
            },
            'MoneyControl': {
                'search_url': 'https://www.moneycontrol.com/news/search/?q={}',
                'rss_url': 'https://www.moneycontrol.com/rss/business.xml',
                'base_url': 'https://www.moneycontrol.com'
            },
            
            # Startup & Tech Focused
            'YourStory': {
                'search_url': 'https://yourstory.com/search?q={}',
                'rss_url': 'https://yourstory.com/feed',
                'base_url': 'https://yourstory.com'
            },
            'Inc42': {
                'search_url': 'https://inc42.com/?s={}',
                'rss_url': 'https://inc42.com/feed/',
                'base_url': 'https://inc42.com'
            },
            'MediaNama': {
                'search_url': 'https://www.medianama.com/?s={}',
                'rss_url': 'https://www.medianama.com/feed/',
                'base_url': 'https://www.medianama.com'
            },
            'Entrackr': {
                'search_url': 'https://entrackr.com/?s={}',
                'rss_url': 'https://entrackr.com/feed/',
                'base_url': 'https://entrackr.com'
            },
            
            # Major Indian News Sites
            'Times of India': {
                'search_url': 'https://timesofindia.indiatimes.com/searchresult.cms?query={}',
                'rss_url': 'https://timesofindia.indiatimes.com/rssfeedstopstories.cms',
                'base_url': 'https://timesofindia.indiatimes.com'
            },
            'Hindustan Times': {
                'search_url': 'https://www.hindustantimes.com/search?q={}',
                'rss_url': 'https://www.hindustantimes.com/feeds/rss/business/rssfeed.xml',
                'base_url': 'https://www.hindustantimes.com'
            },
            'Indian Express': {
                'search_url': 'https://indianexpress.com/search/?q={}',
                'rss_url': 'https://indianexpress.com/section/business/rss/',
                'base_url': 'https://indianexpress.com'
            },
            'NDTV': {
                'search_url': 'https://www.ndtv.com/search?q={}',
                'rss_url': 'https://feeds.feedburner.com/ndtvprofit-latest',
                'base_url': 'https://www.ndtv.com'
            },
            'News18': {
                'search_url': 'https://www.news18.com/search/?q={}',
                'rss_url': 'https://www.news18.com/rss/business.xml',
                'base_url': 'https://www.news18.com'
            }
        }
        
        # Google News RSS for quick commerce with date filtering
        self.google_news_rss = "https://news.google.com/rss/search?q=quick+commerce+OR+q-commerce+OR+blinkit+OR+zepto+OR+instamart&hl=en-US&gl=US&ceid=US:en"

    def _calculate_timeframe(self):
        """Calculate start and end dates based on timeframe"""
        end_date = datetime.now()
        
        if self.timeframe not in self.timeframe_options:
            logger.warning(f"Invalid timeframe '{self.timeframe}', defaulting to '7d'")
            self.timeframe = '7d'
        
        timeframe_config = self.timeframe_options[self.timeframe]
        
        if 'hours' in timeframe_config:
            start_date = end_date - timedelta(hours=timeframe_config['hours'])
        elif 'days' in timeframe_config:
            start_date = end_date - timedelta(days=timeframe_config['days'])
        else:
            # Custom timeframe - can be set via environment variables
            custom_days = int(os.getenv('CUSTOM_DAYS_BACK', 7))
            start_date = end_date - timedelta(days=custom_days)
        
        return start_date, end_date

    def is_article_in_timeframe(self, pub_date_str: str) -> bool:
        """Check if article publication date is within the specified timeframe"""
        if not pub_date_str:
            return True  # Include articles without dates to be safe
        
        try:
            # Simple date parsing without external dependencies
            # Common formats: "Mon, 01 Jan 2024 12:00:00 GMT", "2024-01-01T12:00:00Z"
            pub_date = None
            
            # Try parsing RFC 2822 format (common in RSS)
            if 'GMT' in pub_date_str or 'UTC' in pub_date_str:
                try:
                    from email.utils import parsedate_to_datetime
                    pub_date = parsedate_to_datetime(pub_date_str)
                except:
                    pass
            
            # Try ISO format
            if pub_date is None and 'T' in pub_date_str:
                try:
                    pub_date_str_clean = pub_date_str.replace('Z', '+00:00')
                    pub_date = datetime.fromisoformat(pub_date_str_clean.split('+')[0])
                except:
                    pass
            
            # If we couldn't parse, include the article
            if pub_date is None:
                return True
            
            # Make timezone naive for comparison
            if pub_date.tzinfo is not None:
                pub_date = pub_date.replace(tzinfo=None)
            
            return pub_date >= self.start_date
            
        except Exception as e:
            logger.debug(f"Error parsing date '{pub_date_str}': {str(e)}")
            return True  # Include if we can't parse the date

    def clean_text(self, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ""
        
        # Remove extra whitespace and normalize
        text = re.sub(r'\s+', ' ', text.strip())
        # Remove special characters that might cause issues
        text = re.sub(r'[^\w\s\-.,!?;:()\[\]"\'/@#$%&*+=<>{}|\\`~]', '', text)
        return text

    def extract_article_content(self, url: str) -> Dict[str, str]:
        """Extract full article content from URL"""
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Remove unwanted elements
            for element in soup(['script', 'style', 'nav', 'header', 'footer', 'aside', 'advertisement']):
                element.decompose()
            
            # Try to find article title
            title = ""
            for selector in ['h1', 'title', '.headline', '.article-title', '.entry-title']:
                title_elem = soup.select_one(selector)
                if title_elem:
                    title = self.clean_text(title_elem.get_text())
                    break
            
            # Try to find article content
            content = ""
            content_selectors = [
                'article', '.article-content', '.entry-content', '.post-content',
                '.article-body', '.story-body', '.content', '.main-content',
                '[data-module="ArticleBody"]', '.article-wrap'
            ]
            
            for selector in content_selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    # Get all paragraph text
                    paragraphs = content_elem.find_all(['p', 'div'])
                    content_text = []
                    for p in paragraphs:
                        text = self.clean_text(p.get_text())
                        if len(text) > 50:  # Only include substantial paragraphs
                            content_text.append(text)
                    content = '\n\n'.join(content_text)
                    break
            
            # Fallback: get all paragraph text from the page
            if not content:
                paragraphs = soup.find_all('p')
                content_text = []
                for p in paragraphs:
                    text = self.clean_text(p.get_text())
                    if len(text) > 50:
                        content_text.append(text)
                content = '\n\n'.join(content_text[:10])  # Limit to first 10 paragraphs
            
            return {
                'title': title or 'No title found',
                'content': content or 'No content extracted',
                'url': url
            }
            
        except Exception as e:
            logger.error(f"Error extracting content from {url}: {str(e)}")
            return {
                'title': 'Error extracting title',
                'content': f'Error extracting content: {str(e)}',
                'url': url
            }

    def search_google_news(self) -> List[Dict[str, str]]:
        """Search Google News RSS for quick commerce articles with timeframe filtering"""
        articles = []
        try:
            response = self.session.get(self.google_news_rss, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'xml')
            items = soup.find_all('item')
            
            for item in items[:30]:  # Check more items since we're filtering by date
                title = item.find('title')
                link = item.find('link')
                pub_date = item.find('pubDate')
                description = item.find('description')
                
                if title and link:
                    pub_date_str = pub_date.get_text() if pub_date else ''
                    
                    # Check timeframe first
                    if not self.is_article_in_timeframe(pub_date_str):
                        continue
                    
                    article_data = {
                        'title': self.clean_text(title.get_text()),
                        'url': link.get_text(),
                        'source': 'Google News',
                        'published_date': pub_date_str,
                        'description': self.clean_text(description.get_text()) if description else ''
                    }
                    
                    # Extract full content
                    full_content = self.extract_article_content(article_data['url'])
                    article_data['content'] = full_content['content']
                    
                    articles.append(article_data)
                    
                    # Rate limiting
                    time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error searching Google News: {str(e)}")
        
        return articles

    def search_rss_feeds(self) -> List[Dict[str, str]]:
        """Search RSS feeds from Indian news sources with timeframe filtering"""
        articles = []
        
        for source_name, source_info in self.news_sources.items():
            if 'rss_url' not in source_info:
                continue
                
            try:
                logger.info(f"Searching RSS for {source_name}...")
                response = self.session.get(source_info['rss_url'], timeout=15)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'xml')
                items = soup.find_all('item')
                
                for item in items[:15]:  # Check more items since we're filtering by date
                    title = item.find('title')
                    link = item.find('link')
                    pub_date = item.find('pubDate')
                    description = item.find('description')
                    
                    if title and link:
                        title_text = self.clean_text(title.get_text())
                        pub_date_str = pub_date.get_text() if pub_date else ''
                        
                        # Check timeframe first
                        if not self.is_article_in_timeframe(pub_date_str):
                            continue
                        
                        # Check if title contains quick commerce keywords
                        if any(keyword.lower() in title_text.lower() for keyword in self.keywords):
                            article_data = {
                                'title': title_text,
                                'url': link.get_text(),
                                'source': source_name,
                                'published_date': pub_date_str,
                                'description': self.clean_text(description.get_text()) if description else ''
                            }
                            
                            # Extract full content
                            full_content = self.extract_article_content(article_data['url'])
                            article_data['content'] = full_content['content']
                            
                            articles.append(article_data)
                            
                            # Rate limiting
                            time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error searching RSS for {source_name}: {str(e)}")
                continue
        
        return articles

    def search_news_api(self, query: str) -> List[Dict[str, str]]:
        """Search using NewsAPI with timeframe filtering"""
        articles = []
        api_key = os.getenv('NEWS_API_KEY')
        
        if not api_key:
            logger.warning("NEWS_API_KEY not found in environment variables")
            return articles
        
        try:
            # Convert timeframe to NewsAPI format
            from_date = self.start_date.strftime('%Y-%m-%d')
            to_date = self.end_date.strftime('%Y-%m-%d')
            
            url = f"https://newsapi.org/v2/everything"
            
            params = {
                'q': query,
                'from': from_date,
                'to': to_date,
                'sortBy': 'publishedAt',
                'language': 'en',
                'apiKey': api_key,
                'pageSize': 50
            }
            
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            
            for article in data.get('articles', []):
                if article.get('url'):
                    article_data = {
                        'title': self.clean_text(article.get('title', '')),
                        'url': article.get('url'),
                        'source': article.get('source', {}).get('name', 'Unknown'),
                        'published_date': article.get('publishedAt', ''),
                        'description': self.clean_text(article.get('description', ''))
                    }
                    
                    # Extract full content
                    full_content = self.extract_article_content(article_data['url'])
                    article_data['content'] = full_content['content']
                    
                    articles.append(article_data)
                    
                    # Rate limiting
                    time.sleep(0.5)
                    
        except Exception as e:
            logger.error(f"Error with NewsAPI: {str(e)}")
        
        return articles

    def scrape_all_news(self) -> List[Dict[str, str]]:
        """Main method to scrape all news sources with timeframe filtering"""
        all_articles = []
        
        logger.info(f"Scraping news for timeframe: {self.timeframe_options[self.timeframe]['description']}")
        logger.info(f"Date range: {self.start_date.strftime('%Y-%m-%d %H:%M')} to {self.end_date.strftime('%Y-%m-%d %H:%M')}")
        
        # 1. Search Indian news site RSS feeds
        logger.info("Searching Indian news RSS feeds...")
        rss_articles = self.search_rss_feeds()
        all_articles.extend(rss_articles)
        logger.info(f"Found {len(rss_articles)} articles from RSS feeds")
        
        # 2. Search Google News RSS
        logger.info("Searching Google News RSS...")
        google_articles = self.search_google_news()
        all_articles.extend(google_articles)
        logger.info(f"Found {len(google_articles)} articles from Google News")
        
        # 3. Search using NewsAPI if available
        logger.info("Searching NewsAPI...")
        newsapi_count = 0
        for keyword in ['quick commerce india', 'blinkit', 'zepto', 'swiggy instamart']:
            try:
                news_api_articles = self.search_news_api(keyword)
                all_articles.extend(news_api_articles)
                newsapi_count += len(news_api_articles)
                time.sleep(2)  # Rate limiting
            except Exception as e:
                logger.error(f"Error with NewsAPI for keyword '{keyword}': {str(e)}")
                continue
        
        logger.info(f"Found {newsapi_count} articles from NewsAPI")
        
        # Remove duplicates based on URL and title similarity
        unique_articles = self.remove_duplicates(all_articles)
        
        logger.info(f"Total unique articles found: {len(unique_articles)}")
        return unique_articles

    def remove_duplicates(self, articles: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Remove duplicate articles based on URL and title similarity"""
        seen_urls = set()
        seen_titles = set()
        unique_articles = []
        
        for article in articles:
            url = article.get('url', '')
            title = article.get('title', '').lower().strip()
            
            # Skip if URL already seen
            if url in seen_urls:
                continue
            
            # Skip if very similar title already seen
            title_words = set(title.split())
            is_duplicate = False
            for seen_title in seen_titles:
                seen_words = set(seen_title.split())
                # If more than 70% words match, consider duplicate
                if len(title_words) > 0 and len(seen_words) > 0:
                    overlap = len(title_words & seen_words) / max(len(title_words), len(seen_words))
                    if overlap > 0.7:
                        is_duplicate = True
                        break
            
            if not is_duplicate:
                seen_urls.add(url)
                seen_titles.add(title)
                unique_articles.append(article)
        
        return unique_articles

    def save_to_text_file(self, articles: List[Dict[str, str]], filename: str = None):
        """Save articles to a text file with timeframe info"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            timeframe_label = self.timeframe.replace('h', 'hours').replace('d', 'days')
            filename = f"quick_commerce_news_{timeframe_label}_{timestamp}.txt"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write("QUICK COMMERCE INDUSTRY NEWS REPORT\n")
                f.write("=" * 50 + "\n")
                f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Timeframe: {self.timeframe_options[self.timeframe]['description']}\n")
                f.write(f"Date range: {self.start_date.strftime('%Y-%m-%d %H:%M')} to {self.end_date.strftime('%Y-%m-%d %H:%M')}\n")
                f.write(f"Total articles: {len(articles)}\n\n")
                
                # Group articles by source for better organization
                articles_by_source = defaultdict(list)
                for article in articles:
                    articles_by_source[article.get('source', 'Unknown')].append(article)
                
                f.write("ARTICLES BY SOURCE:\n")
                for source, source_articles in articles_by_source.items():
                    f.write(f"• {source}: {len(source_articles)} articles\n")
                f.write("\n")
                
                for i, article in enumerate(articles, 1):
                    f.write(f"\n{'='*80}\n")
                    f.write(f"ARTICLE {i}\n")
                    f.write(f"{'='*80}\n\n")
                    f.write(f"TITLE: {article.get('title', 'No title')}\n\n")
                    f.write(f"SOURCE: {article.get('source', 'Unknown')}\n\n")
                    f.write(f"URL: {article.get('url', '')}\n\n")
                    f.write(f"PUBLISHED: {article.get('published_date', 'Unknown')}\n\n")
                    
                    if article.get('description'):
                        f.write(f"DESCRIPTION:\n{article['description']}\n\n")
                    
                    f.write(f"FULL CONTENT:\n")
                    f.write("-" * 40 + "\n")
                    f.write(f"{article.get('content', 'No content available')}\n")
                    f.write("-" * 40 + "\n\n")
            
            logger.info(f"Articles saved to {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error saving to text file: {str(e)}")
            return None

    def save_to_json(self, articles: List[Dict[str, str]], filename: str = None):
        """Save articles to JSON file with timeframe info"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            timeframe_label = self.timeframe.replace('h', 'hours').replace('d', 'days')
            filename = f"quick_commerce_news_{timeframe_label}_{timestamp}.json"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump({
                    'generated_on': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'timeframe': self.timeframe_options[self.timeframe]['description'],
                    'date_range': {
                        'start': self.start_date.strftime('%Y-%m-%d %H:%M:%S'),
                        'end': self.end_date.strftime('%Y-%m-%d %H:%M:%S')
                    },
                    'total_articles': len(articles),
                    'articles': articles
                }, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Articles saved to {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Error saving to JSON file: {str(e)}")
            return None

def parse_timeframe_argument():
    """Parse timeframe from command line arguments or environment variables"""
    parser = argparse.ArgumentParser(description='Quick Commerce News Scraper')
    parser.add_argument(
        '--timeframe', '-t',
        choices=['6h', '12h', '24h', '2d', '3d', '7d', '14d', '30d', '60d', '90d', 'custom'],
        default=os.getenv('SCRAPE_TIMEFRAME', '7d'),
        help='Timeframe for news scraping (default: 7d)'
    )
    parser.add_argument(
        '--custom-days',
        type=int,
        default=int(os.getenv('CUSTOM_DAYS_BACK', 7)),
        help='Number of days for custom timeframe (default: 7)'
    )
    parser.add_argument(
        '--list-timeframes',
        action='store_true',
        help='List all available timeframe options'
    )
    
    try:
        args = parser.parse_args()
    except SystemExit:
        # If argument parsing fails, use defaults
        class DefaultArgs:
            timeframe = os.getenv('SCRAPE_TIMEFRAME', '7d')
            custom_days = int(os.getenv('CUSTOM_DAYS_BACK', 7))
            list_timeframes = False
        args = DefaultArgs()
    
    if hasattr(args, 'list_timeframes') and args.list_timeframes:
        print("Available timeframe options:")
        scraper = QuickCommerceNewsScraper()
        for code, info in scraper.timeframe_options.items():
            print(f"  {code:6} - {info['description']}")
        exit(0)
    
    # Set custom days if using custom timeframe
    if args.timeframe == 'custom':
        os.environ['CUSTOM_DAYS_BACK'] = str(args.custom_days)
    
    return args.timeframe

def main():
    """Main function to run the scraper with timeframe selection"""
    
    try:
        # Parse timeframe from arguments or environment
        timeframe = parse_timeframe_argument()
        
        scraper = QuickCommerceNewsScraper(timeframe=timeframe)
        
        logger.info(f"Starting quick commerce news scraping...")
        logger.info(f"Timeframe: {scraper.timeframe_options[timeframe]['description']}")
        
        # Scrape all news
        articles = scraper.scrape_all_news()
        
        if articles:
            # Save to text file (main requirement)
            text_filename = scraper.save_to_text_file(articles)
            
            # Also save to JSON for structured data
            json_filename = scraper.save_to_json(articles)
            
            if text_filename:
                print(f"\n✅ Successfully scraped {len(articles)} articles!")
                print(f"📅 Timeframe: {scraper.timeframe_options[timeframe]['description']}")
                print(f"📄 Text file: {text_filename}")
                if json_filename:
                    print(f"📊 JSON file: {json_filename}")
            else:
                print("❌ Error saving files")
                
        else:
            logger.warning("No articles found for the specified timeframe")
            print("⚠️ No articles found for the specified timeframe")
            
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        print(f"❌ Error: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()
