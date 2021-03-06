# data importing and cleaning ####

library(tidyverse)
library(dplyr)
library(countrycode)
library(shiny)
library(shinydashboard)
library(plotly)
library(shinyWidgets)
library(rworldmap)
library(rsconnect)

theme_set(theme_light())

# remove 'HDI for year' for too many missing data
# remove 'generation' for redundancy to age variable
# rename some of the variables

data = read_csv('master.csv')

data = data %>% 
  select(-c('HDI for year', 'generation')) %>%
  rename(gdp_for_year = 'gdp_for_year ($)',
         gdp_per_capita = 'gdp_per_capita ($)',
         country_year = 'country-year',
         suicide_100k = 'suicides/100k pop') %>% 
  as.data.frame()

# by scanning the data, there seems to be 12 rows for each country_year combination

data %>%
  group_by(country_year) %>%
  count() %>%
  filter(n != 12)

# a lot of the countries are missing data in 2016, and there are missing countries as well
# remove redundant country_year variable

data = data %>% 
  filter(year != 2016) %>% 
  select(-country_year)

# examine country data completeness by year

min_year = data %>% 
  group_by(country) %>% 
  summarize(row = n(),
            years = row / 12) %>% 
  arrange(years)

# there are seven countries with <= 3 years of data

data = data %>% 
  filter(!(country %in% head(min_year$country, 7)))


# tidy variables

data$sex = ifelse(data$sex == 'male', 'Male', 'Female')
data$age = gsub(' years', '', data$age)

# get continent data

data$continent = countrycode(sourcevar = data[, 'country'],
                             origin = 'country.name',
                             destination = 'continent')

# create factors

data$country = factor(data$country)
data$sex = factor(data$sex)
data$continent = factor(data$continent)

# making age ordinal for consistent graphical presentation

data$age = factor(data$age,
                  ordered = T,
                  levels = c('5-14',
                             '15-24', 
                             '25-34', 
                             '35-54', 
                             '55-74', 
                             '75+'))

global_average = (sum(as.numeric(data$suicides_no)) / sum(as.numeric(data$population))) * 100000

glimpse(data)
names(data)


# Worldwide Tab ####

# 1.1) Global Line Plot
gg = data %>% 
  group_by(year) %>%
  summarize(population = sum(population), 
            suicides = sum(suicides_no), 
            suicides_per_100k = (suicides / population) * 100000) %>% 
  ggplot(aes(year, suicides_per_100k)) +
  geom_line(col = 'skyblue', size = 1) +
  geom_point(col = 'skyblue', size = 2) +
  geom_hline(col = 'grey', yintercept = global_average, linetype = 2) +
  labs(title = 'Global Suicides Trend Over Time',
       x = 'Year',
       y = 'Suicides (per 100k)') + 
  scale_x_continuous(breaks = seq(1985,2015,5))
  
global_line = ggplotly(gg) 
global_line

# 1.2) Global Line Plot by Gender
ggg = data %>% 
  group_by(year, sex) %>%
  summarize(population = sum(population), 
            suicides = sum(suicides_no), 
            suicides_per_100k = (suicides / population) * 100000) %>% 
  ggplot(aes(year, suicides_per_100k, col = sex)) +
  geom_line(size = 1) +
  geom_point(size = 2) +
  labs(title = 'Global Suicides Trend Over Time, by Sex',
       x = 'Year',
       y = 'Suicides (per 100k)') + 
  scale_x_continuous(breaks = seq(1985,2015,5))

global_line_sex = ggplotly(ggg)
global_line_sex

# 1.3) Global Line Plot by Age
gggg = data %>% 
  group_by(year, age) %>%
  summarize(population = sum(population), 
            suicides = sum(suicides_no), 
            suicides_per_100k = (suicides / population) * 100000) %>% 
  ggplot(aes(year, suicides_per_100k, col = age)) +
  geom_line(size = 1) +
  geom_point(size = 2) +
  labs(title = 'Global Suicides Trend Over Time, by Age',
       x = 'Year',
       y = 'Suicides (per 100k)') + 
  scale_x_continuous(breaks = seq(1985,2015,5))

global_line_age = ggplotly(gggg)
global_line_age

# 1.4) Global Pie Chart by Gender
g4_plotly = data %>% 
  group_by(sex) %>%
  summarize(population = sum(population), 
            suicides = sum(suicides_no), 
            suicides_per_100k = (suicides / population) * 100000)

# ggggg_plotly$color = factor(ggggg_plotly$sex,
#                             labels = c('tomato', 'deepskyblue'))

global_pie_sex = 
  plot_ly(g4_plotly, labels = ~sex, values = ~suicides_per_100k,
          type = 'pie') %>% 
  layout(title = "Global suicides (per 100k), by Sex",
         xaxis = list(showgrid = FALSE, zeroline = FALSE, showticklabels = FALSE),
         yaxis = list(showgrid = FALSE, zeroline = FALSE, showticklabels = FALSE))

global_pie_sex

# 1.5) Global Pie Chart by Age
g5_plotly = data %>% 
  group_by(age) %>%
  summarize(population = sum(population), 
            suicides = sum(suicides_no), 
            suicides_per_100k = (suicides / population) * 100000)

global_pie_age =
  plot_ly(g5_plotly, labels = ~age, values = ~suicides_per_100k,
          type = 'pie') %>% 
  layout(title = "Global suicides (per 100k), by Age",
         xaxis = list(showgrid = FALSE, zeroline = FALSE, showticklabels = FALSE),
         yaxis = list(showgrid = FALSE, zeroline = FALSE, showticklabels = FALSE))
  
global_pie_age



# Countries Tab ####

# 2.1) Country barplot
g12 = data %>% 
  group_by(country, continent) %>% 
  summarize(population = sum(population), 
            suicides = sum(suicides_no), 
            suicides_per_100k = (suicides / population) * 100000) %>% 
  arrange(desc(suicides_per_100k))

g12$country <- factor(g12$country, 
                          ordered = T, 
                          levels = rev(g12$country))
            
g12_plotly = ggplot(g12, aes(country, suicides_per_100k, fill = suicides_per_100k)) + 
  geom_bar(stat = "identity") + 
  geom_hline(yintercept = global_average, linetype = 2, color = "grey", size = 1) +
  labs(title = "Global Suicides per 100k, by Country",
       x = "Country", 
       y = "Suicides per 100k", 
       fill = "Continent") +
  coord_flip() +
  scale_y_continuous(breaks = seq(0, 45, 2)) + 
  theme(legend.position = "none")

bar_country = ggplotly(g12_plotly)
bar_country


# 2.2) Country heatmap
heatmap <- data %>%
  group_by(country) %>%
  summarize(population = sum(population), 
            suicides = sum(suicides_no), 
            suicides_per_100k = (suicides / population) * 100000)

countrydata <- joinCountryData2Map(heatmap, joinCode = "NAME", nameJoinColumn = "country")

par(mar=c(0, 0, 0, 0)) # margins

mapCountryData(countrydata, 
                nameColumnToPlot="suicides_per_100k", 
                mapTitle="", 
                oceanCol="skyblue", 
                missingCountryCol="white", 
                catMethod = "pretty")


# GDP Tab ####

# 3.1) Correlation between GDP and suicide by year
g6_plotly = data %>% 
  group_by(country, continent, year) %>% 
  summarize(population = sum(population), 
            suicides = sum(suicides_no), 
            suicides_per_100k = (suicides / population) * 100000,
            gdp_per_capita = mean(gdp_per_capita)
)

g7_plotly = ggplot(g6_plotly, aes(x = gdp_per_capita, y = suicides_per_100k, col = continent)) + 
  geom_point(aes(size = population, frame = year, ids = country)) +
  scale_x_log10(labels=scales::dollar_format(prefix="$")) +
  labs(title = "Correlation between GDP (per capita) and Suicides per 100k by year", 
       x = "GDP (per capita)", 
       y = "Suicides per 100k"
       ) 

GDP_suicide_scatter_year = ggplotly(g7_plotly)
GDP_suicide_scatter_year

# 3.2) Correlation between GDP and suicide across years

# a) data cleaning for linear regression
g8_plotly = data %>% 
  group_by(country, continent) %>% 
  summarize(population = sum(population), 
            suicides = sum(suicides_no), 
            suicides_per_100k = (suicides / population) * 100000,
            gdp_per_capita = mean(gdp_per_capita)
  )

model = lm(suicides_per_100k ~ gdp_per_capita, g8_plotly)
plot(model, 1)
levels(data$country)

# Remove the following outliers

g9_plotly = g8_plotly %>% 
  filter(!(country %in% c("Lithuania", "Russian Federation", "Sri Lanka")))

model1 = lm(suicides_per_100k ~ gdp_per_capita, g9_plotly)

# Delete points with large Cook's Distance

g9_plotly$cooks_dist = cooks.distance(model1)

g10_plotly = g9_plotly %>% 
  filter(cooks_dist < 4/nrow(g9_plotly))

model2 = lm(suicides_per_100k ~ gdp_per_capita, g10_plotly)

# Check linear regression assumption
shapiro.test(model2$residuals) # normality of the residuals not satisfied, the chosen regression might not be a good fit

# linear regression model
summary(model2)

# b) plotting
g11_plotly = ggplot(g10_plotly, aes(x = gdp_per_capita, y = suicides_per_100k, col = continent)) + 
  geom_point() +
  geom_smooth(method = "lm", aes(group = 1)) +
  scale_x_continuous(labels=scales::dollar_format(prefix="$"), breaks = seq(0, 70000, 10000)) +
  labs(title = "Correlation between GDP (per capita) and Suicides per 100k across years", 
       x = "GDP (per capita)", 
       y = "Suicides per 100k")
  
GDP_suicide_scatter_allyear = ggplotly(g11_plotly)
GDP_suicide_scatter_allyear

# Suicide = 8.436 + 0.118*GDP
# An increase in GDP per capita by $8475 was associated with 1
# additional suicide, per 100k people, per year (p = 0.0164, R^2 = 0.05489)

# Datatable Tab ####

# About Tab ####


