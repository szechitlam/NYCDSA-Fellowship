# -*- coding: utf-8 -*-

# These itmes will hold the reviews scraped from rottentomatoes

# movie represents the name of the movie
# critic_name represents the name of the critics
# company represents the company that the critic works for
# rating represents the binary ratings (good or bad)
# review_text represents the synopsis of the full review


import scrapy

class MovieReviewItem(scrapy.Item):
    movie = scrapy.Field()
    critic_name = scrapy.Field()
    company = scrapy.Field()
    rating = scrapy.Field()
    review_text = scrapy.Field()
