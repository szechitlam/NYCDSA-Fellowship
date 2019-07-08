# -*- coding: utf-8 -*-

# These itmes will hold the value scraped from rottentomatoes

# movie represents the name of the movie
# num_critic represents the number of critic reviews
# num_audience represents the number of audience reviews
# critic_prop represents the proportion of fresh tomatos given by critics
# audience_prop represents the proportion of audience ratings >= 3.5 stars


import scrapy

class MovieReviewItem(scrapy.Item):
    movie = scrapy.Field()
    critic_prop = scrapy.Field()
    num_critic = scrapy.Field()
    audience_prop = scrapy.Field()
    num_audience = scrapy.Field()
