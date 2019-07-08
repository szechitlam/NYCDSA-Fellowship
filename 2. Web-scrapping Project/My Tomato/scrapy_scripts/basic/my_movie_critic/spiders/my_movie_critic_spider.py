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
            yield response.follow(url = link, callback = self.parse2)

    def parse2(self, response):
        score_block = response.xpath('//div[@class="mop-ratings-wrap score_panel js-mop-ratings-wrap"]')
        movie = score_block.css('h1::text').extract_first()
        critic_prop = score_block.css('span.mop-ratings-wrap__percentage ::text').extract()[0].strip()
        audience_prop = score_block.css('span.mop-ratings-wrap__percentage ::text').extract()[1].strip()
        num_critic = score_block.css('small.mop-ratings-wrap__text--small::text').extract_first().strip()
        num_audience = response.xpath('//*[@id="topSection"]/div[2]/div[1]/section/section/div[2]/div/strong/text()').extract_first().strip()

        item = MovieReviewItem()
        item['movie'] = movie
        item['critic_prop'] = critic_prop
        item['audience_prop'] = audience_prop
        item['num_critic'] = num_critic
        item['num_audience'] = num_audience

        yield item
