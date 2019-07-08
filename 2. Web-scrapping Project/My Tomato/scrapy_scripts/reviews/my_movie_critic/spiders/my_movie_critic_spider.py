from scrapy import Spider, Request
from my_movie_critic.items import MovieReviewItem
import re

# Create the Spider Class
class MovieReviewSpider(Spider):
    name = 'movie_review_spider'
    # start_requests method
    def start_requests(self):
        yield Request(url = 'https://editorial.rottentomatoes.com/guide/all-pixar-movies-ranked/', callback = self.parse)

    def parse(self, response):
        links = response.xpath('//div[contains(@id,"row-index-")]').css('h2 > a::attr(href)').extract()
        # Follow each of the extracted links
        for link in links:
            yield response.follow(url = link + 'reviews', callback = self.parse2)

    def parse2(self, response):
        movie = response.xpath('//*[@id="main_container"]/div[1]/section/div/div[1]/h2/a/text()').extract_first().strip()
        reviews = response.xpath('//div[@class="row review_table_row"]')

        for review in reviews:
            critic_name = review.xpath('.//div[@class="col-sm-13 col-xs-24 col-sm-pull-4 critic_name"]/a/text()').extract_first()
            company = review.xpath('.//div[@class="col-sm-13 col-xs-24 col-sm-pull-4 critic_name"]/a/em/text()').extract_first()
            rating = re.sub('review_icon icon small ', '', review.xpath('.//div[@class="col-xs-16 review_container"]/div/@class').extract_first())
            review_text = review.xpath('.//div[@class="the_review"]/text()').extract_first().strip().lower()

            item = MovieReviewItem()
            item['movie'] = movie
            item['critic_name'] = critic_name
            item['company'] = company
            item['rating'] = rating
            item['review_text'] = review_text

            yield item

        # Extract number of pages for each movie and follow along
        num_pages = re.sub('Page 1 of ', '', response.xpath('//*[@id="content"]/div/div/div/div[1]/span/text()').extract_first())
        page_urls = [response.url + '?page={}'.format(x) for x in range(2, int(num_pages) + 1)]

        for url in page_urls:
            yield response.follow(url = url, meta = {'movie': movie}, callback = self.parse3)

    def parse3(self, response):
        movie = response.meta['movie']
        reviews = response.xpath('//div[@class="row review_table_row"]')

        for review in reviews:
            critic_name = review.xpath('.//div[@class="col-sm-13 col-xs-24 col-sm-pull-4 critic_name"]/a/text()').extract_first()
            company = review.xpath('.//div[@class="col-sm-13 col-xs-24 col-sm-pull-4 critic_name"]/a/em/text()').extract_first()
            rating = re.sub('review_icon icon small ', '', review.xpath('.//div[@class="col-xs-16 review_container"]/div/@class').extract_first())
            review_text = review.xpath('.//div[@class="the_review"]/text()').extract_first().strip().lower()

            item = MovieReviewItem()
            item['movie'] = movie
            item['critic_name'] = critic_name
            item['company'] = company
            item['rating'] = rating
            item['review_text'] = review_text

            yield item
