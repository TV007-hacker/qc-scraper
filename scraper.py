#!/usr/bin/env python3
"""
Quick Commerce Industry News Scraper - Fixed Version
Focuses on direct RSS feeds and NewsAPI for actual content
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
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
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
            '90d': {'days': 90, 'description': 'Last 3 months'}
        }
        
        self.start_date, self.end_date = self._calculate_timeframe()
        
        # Keywords for quick commerce
        self.keywords = [
            'quick commerce', 'q-commerce', 'quick-commerce', 'qcommerce',
            'blinkit', 'zepto', 'swiggy instamart', 'instamart',
            'amazon now', 'flipkart minutes', 'bigbasket now',
            'dunzo', 'grofers', 'ultra fast delivery', '10 minute delivery',
            'instant delivery', 'rapid delivery', 'dark store', 'dark stores'
        ]

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
        
        return start_date, end_date

    def is_article_in_timeframe(self, pub_date_str: str) -> bool:
        """Check if article publication date is within the specified timeframe"""
        if not pub_date_str:
            return True
        
        try:
            from email.utils import parsedate_to_datetime
            pub_date = parsedate_to_datetime(pub_date_str)
            if pub_date.tzinfo is not None:
                pub_date = pub_date.replace(tzinfo=None)
            return pub_date >= self.start_date
        except:
            return True

    def clean_text(self, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ""
        text = re.sub(r'\s+', ' ', text.strip())
        return text

    def extract_content_from_url(self, url: str) -> str:
        """Extract article content from direct URL with robust extraction"""
        try:
            # Skip Google News and other redirect URLs
            if any(domain in url for domain in ['news.google.com', 'google.com/url', 't.co']):
                return "Redirect URL - content extraction skipped"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Connection': 'keep-alive'
            }
            
            response = self.session.get(url, headers=headers, timeout=20, allow_redirects=True)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Remove unwanted elements
            for element in soup(['script', 'style', 'nav', 'header', 'footer', 'aside', 'advertisement', 'form', 'button']):
                element.decompose()
            
            # Try multiple content extraction methods
            content_text = []
            
            # Method 1: Find article content containers
            article_selectors = [
                'article', '.article-content', '.entry-content', '.post-content',
                '.article-body', '.story-body', '.content', '.main-content',
                '[data-module="ArticleBody"]', '.story-text', '.article-text'
            ]
            
            for selector in article_selectors:
                article_elem = soup.select_one(selector)
                if article_elem:
                    paragraphs = article_elem.find_all(['p', 'div'])
                    for p in paragraphs:
                        text = self.clean_text(p.get_text())
                        if len(text) > 50:
                            content_text.append(text)
                    if content_text:
                        break
            
            # Method 2: If no article content found, get all paragraphs
            if not content_text:
                all_paragraphs = soup.find_all('p')
                for p in all_paragraphs:
                    text = self.clean_text(p.get_text())
                    if (len(text) > 50 and 
                        not any(unwanted in text.lower() for unwanted in [
                            'subscribe', 'newsletter', 'advertisement', 'cookie', 'privacy'
                        ])):
                        content_text.append(text)
            
            # Return the extracted content
            if content_text:
                return '\n\n'.join(content_text[:15])  # First 15 good paragraphs
            else:
                return "Content extraction failed - unable to locate article text"
                
        except Exception as e:
            logger.error(f"Error extracting content from {url}: {str(e)}")
            return f"Error extracting content: {str(e)}"

    def search_direct_rss_feeds(self) -> List[Dict[str, str]]:
        """Search direct RSS feeds from Indian news sources"""
        articles = []
        
        # Working RSS feeds for Indian news sources
        rss_sources = {
            'Economic Times': 'https://economictimes.indiatimes.com/rssfeedsdefault.cms',
            'Business Standard': 'https://www.business-standard.com/rss/latest.rss',
            'LiveMint': 'https://www.livemint.com/rss/companies',
            'Financial Express': 'https://www.financialexpress.com/feed/',
            'YourStory': 'https://yourstory.com/feed',
            'Inc42': 'https://inc42.com/feed/',
            'MoneyControl': 'https://www.moneycontrol.com/rss/business.xml'
        }
        
        for source_name, rss_url in rss_sources.items():
            try:
                logger.info(f"Fetching RSS from {source_name}...")
                response = self.session.get(rss_url, timeout=15)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'xml')
                items = soup.find_all('item')
                
                for item in items[:30]:  # Check more items
                    title_elem = item.find('title')
                    link_elem = item.find('link')
                    pub_date_elem = item.find('pubDate')
                    description_elem = item.find('description')
                    
                    if title_elem and link_elem:
                        title = self.clean_text(title_elem.get_text())
                        article_url = link_elem.get_text()
                        pub_date = pub_date_elem.get_text() if pub_date_elem else ''
                        
                        # Check timeframe
                        if not self.is_article_in_timeframe(pub_date):
                            continue
                        
                        # Check if title contains quick commerce keywords
                        if any(keyword.lower() in title.lower() for keyword in self.keywords):
                            
                            # Extract full content from the article URL
                            content = self.extract_content_from_url(article_url)
                            
                            article_data = {
                                'title': title,
                                'url': article_url,
                                'source': source_name,
                                'published_date': pub_date,
                                'description': self.clean_text(description_elem.get_text()) if description_elem else '',
                                'content': content
                            }
                            
                            articles.append(article_data)
                            time.sleep(2)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error with RSS from {source_name}: {str(e)}")
                continue
        
        return articles

    def search_news_api(self, query: str) -> List[Dict[str, str]]:
        """Search using NewsAPI which provides full content"""
        articles = []
        api_key = os.getenv('NEWS_API_KEY')
        
        if not api_key:
            logger.warning("NEWS_API_KEY not found - skipping NewsAPI")
            return articles
        
        try:
            from_date = self.start_date.strftime('%Y-%m-%d')
            to_date = self.end_date.strftime('%Y-%m-%d')
            
            url = "https://newsapi.org/v2/everything"
            params = {
                'q': query,
                'from': from_date,
                'to': to_date,
                'sortBy': 'publishedAt',
                'language': 'en',
                'apiKey': api_key,
                'pageSize': 50,
                'domains': 'economictimes.indiatimes.com,business-standard.com,livemint.com,yourstory.com,inc42.com'
            }
            
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            for article in data.get('articles', []):
                if article.get('url') and article.get('content'):
                    # NewsAPI provides content directly
                    content = article.get('content', '')
                    if '[+' in content:  # Remove NewsAPI truncation markers
                        content = content.split('[+')[0]
                    
                    article_data = {
                        'title': self.clean_text(article.get('title', '')),
                        'url': article.get('url'),
                        'source': article.get('source', {}).get('name', 'NewsAPI'),
                        'published_date': article.get('publishedAt', ''),
                        'description': self.clean_text(article.get('description', '')),
                        'content': content or 'NewsAPI content not available'
                    }
                    
                    articles.append(article_data)
                    
        except Exception as e:
            logger.error(f"Error with NewsAPI: {str(e)}")
        
        return articles

    def scrape_all_news(self) -> List[Dict[str, str]]:
        """Main scraping method - focus on sources with full content"""
        all_articles = []
        
        logger.info(f"Scraping news for: {self.timeframe_options[self.timeframe]['description']}")
        
        # 1. Direct RSS feeds (best for content extraction)
        logger.info("Searching direct RSS feeds...")
        rss_articles = self.search_direct_rss_feeds()
        all_articles.extend(rss_articles)
        logger.info(f"Found {len(rss_articles)} articles from RSS feeds")
        
        # 2. NewsAPI (provides full content)
        logger.info("Searching NewsAPI...")
        newsapi_count = 0
        for keyword in ['quick commerce india', 'blinkit', 'zepto', 'swiggy instamart']:
            try:
                news_api_articles = self.search_news_api(keyword)
                all_articles.extend(news_api_articles)
                newsapi_count += len(news_api_articles)
                time.sleep(2)
            except Exception as e:
                logger.error(f"NewsAPI error for '{keyword}': {str(e)}")
                continue
        
        logger.info(f"Found {newsapi_count} articles from NewsAPI")
        
        # Remove duplicates
        unique_articles = self.remove_duplicates(all_articles)
        
        logger.info(f"Total unique articles: {len(unique_articles)}")
        return unique_articles

    def remove_duplicates(self, articles: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Remove duplicate articles"""
        seen_urls = set()
        unique_articles = []
        
        for article in articles:
            url = article.get('url', '')
            if url not in seen_urls:
                seen_urls.add(url)
                unique_articles.append(article)
        
        return unique_articles

    def save_to_text_file(self, articles: List[Dict[str, str]], filename: str = None):
        """Save articles to text file"""
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
                
                # Group by source
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
            logger.error(f"Error saving file: {str(e)}")
            return None

def parse_timeframe_argument():
    """Parse timeframe from command line"""
    parser = argparse.ArgumentParser(description='Quick Commerce News Scraper')
    parser.add_argument(
        '--timeframe', '-t',
        choices=['6h', '12h', '24h', '2d', '3d', '7d', '14d', '30d', '60d', '90d'],
        default=os.getenv('SCRAPE_TIMEFRAME', '7d'),
        help='Timeframe for news scraping (default: 7d)'
    )
    
    try:
        args = parser.parse_args()
    except SystemExit:
        class DefaultArgs:
            timeframe = os.getenv('SCRAPE_TIMEFRAME', '7d')
        args = DefaultArgs()
    
    return args.timeframe

def main():
    """Main function"""
    try:
        timeframe = parse_timeframe_argument()
        scraper = QuickCommerceNewsScraper(timeframe=timeframe)
        
        logger.info(f"Starting quick commerce news scraping...")
        logger.info(f"Timeframe: {scraper.timeframe_options[timeframe]['description']}")
        
        articles = scraper.scrape_all_news()
        
        if articles:
            text_filename = scraper.save_to_text_file(articles)
            
            if text_filename:
                print(f"\n✅ Successfully scraped {len(articles)} articles!")
                print(f"📅 Timeframe: {scraper.timeframe_options[timeframe]['description']}")
                print(f"📄 Text file: {text_filename}")
                
                # Show content preview for verification
                content_with_text = [a for a in articles if len(a.get('content', '')) > 100]
                print(f"📝 Articles with substantial content: {len(content_with_text)}")
            else:
                print("❌ Error saving file")
        else:
            logger.warning("No articles found")
            print("⚠️ No articles found for the specified timeframe")
            
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        print(f"❌ Error: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main()
