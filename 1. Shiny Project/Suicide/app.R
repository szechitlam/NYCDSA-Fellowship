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
# UI ####

header <- dashboardHeader(title = 'Suicide Rate Analysis',
                          dropdownMenu(
                              type = "messages",
                              messageItem(
                                  from = "Nelson",
                                  message = "Welcome!"
                              ),
                              messageItem(
                                  from = 'Nelson',
                                  message = 'Hope you will gain much insight!'
                              )  
                          )
)


sidebar = dashboardSidebar(
    sidebarMenu(
        menuItem('Worldwide', tabName = 'worldwide', icon = icon('globe',
                                                                 class = 'fa-spin')),
        menuItem('Countries', tabName = 'countries', icon = icon('flag',)),
        menuItem('GDP', tabName = 'gdp', icon = icon('dollar-sign')),
        menuItem('Datatable', tabName = 'datatable', icon = icon('table')),
        menuItem('About', tabName = 'about', icon = icon('address-card'))
    )
)


body = dashboardBody(
    tabItems(
        tabItem(tabName = 'worldwide',
                fluidRow(column(width = 4,
                                valueBox(round(global_average, 2), 'Suicides per 100k',
                                         icon = icon('dizzy'), color = 'red', width = NULL),
                                box(textOutput('introduction'), width = NULL,
                                    h3('Welcome to this suicide rate analysis!'),
                                    h4('Approximately 0.5% of people die by suicide.
                                    Have you ever wonder why does it happens? What goes wrong?
                                    Why is it more common in some places than others?'),
                                    h4("Today, this dashboard will guide you through the various risk factors
                                    that contribute to suicide rates across the globe over
                                    1985 - 2015 and hope to answer some of the questions you might have!")
                                )),
                         box(plotlyOutput('pie_sex'), width = 4),
                         box(plotlyOutput('pie_age'), width = 4)),
                fluidRow(tabBox(width = 10,
                                tabPanel('Overall Global Suicide', plotlyOutput('global_line')),
                                tabPanel('Global Suicide, Stratified by Sex', plotlyOutput('sex_line')),
                                tabPanel('Global Suicide, Stratified by Age', plotlyOutput('age_line'))
                )
                )
        ),
        
        tabItem(tabName = 'datatable', h2('Subset your own dataset!'),
                selectizeInput('dt_continent', 'Continent',
                               choices = levels(data$continent), multiple = T),
                numericRangeInput('dt_gdp', 'GDP per capita', value = c(250, 130000)),
                sliderInput('dt_year', 'Year', value = c(1985, 2015),
                            min = 1985, max = 2015),
                selectizeInput('dt_age', 'Age Group', 
                               choices = levels(data$age), multiple = T),
                checkboxGroupInput('dt_sex', 'Gender', choices = levels(data$sex)),
                downloadButton('download_data'),
                DT::dataTableOutput('datatable')),
        
        tabItem(tabName = 'gdp', h2('Is GDP correlated with suicide rates on a country level?'),
                fluidRow(tabBox(width = 12,
                                tabPanel("Correlation Between GDP (per capita)
                             and Suicides per 100k Across Years",
                                         fluidRow(
                                             box(plotlyOutput('scatter_static'), width = 8),
                                             box(textOutput('regression'), width = 4,
                                                 h2('Regression Analysis'),
                                                 h4('After removing outliers and influential points,
                                        a linear regression was performed to assess the linear relationship
                                        between GDP and suicide rate. The model could be represented
                                        by the following equation:'),
                                                 h3('Suicides = 8.436 + 0.118 (GDP)'),
                                                 h5('where suicides = suicides per 100k, &'),
                                                 h5('GDP = GDP per capita (in thousands USD)'),
                                                 h3('Conclusion'),
                                                 h4('An increase in GDP per capita by $8475 was associated with 1
                                         additional suicide, per 100k people, per year 
                                         (p = 0.0164, R^2 = 0.05489)')
                                             ))),
                                tabPanel('Correlation Animation Across Years',
                                         plotlyOutput('scatter_animation'))
                ))),
        
        tabItem(tabName = 'about',
                textOutput('final'),
                h1('Thank you for visiting this dashboard!'),
                h1(' '),
                h1(' '),
                h5('The dataset used to create this dashboard can be found at:'),
                h5(a("Link", href="https://www.kaggle.com/russellyates88/suicide-rates-overview-1985-to-2016")),
                img(src='https://upload.wikimedia.org/wikipedia/commons/a/a9/Double-alaskan-rainbow-airbrushed.jpg', width = '100%'),
                h6('Photo credited to Eric Rolph')
        ),
        
        tabItem(tabName = 'countries',
                fluidRow(box(plotlyOutput('country_bar', height = '1200px'), width = 7, height = 1300),
                         box(plotOutput('heatmap'), width = 5)
                         
                ))
    )
    
)


ui = dashboardPage(header, sidebar, body)

# Server ####

server <- function(input, output) {
    
    # Worldwide Tab
    output$global_line = renderPlotly({
        global_line
    })
    
    output$pie_sex = renderPlotly({
        global_pie_sex
    })
    
    output$pie_age = renderPlotly({
        global_pie_age
    })
    
    output$sex_line = renderPlotly({
        global_line_sex
    })
    
    output$age_line = renderPlotly({
        global_line_age
    })
    
    # Country tab
    output$country_bar = renderPlotly({
        bar_country
    })
    
    output$heatmap = renderPlot({
        mapParams <- mapPolys(countrydata,
                              nameColumnToPlot="suicides_per_100k",
                              mapTitle="",
                              oceanCol="skyblue",
                              missingCountryCol="white",
                              catMethod = "pretty")
        mtext("White Color: No Data Available",side=1,line=-1)
    })
    
    # GDP tab
    output$scatter_animation = renderPlotly({
        GDP_suicide_scatter_year
    })
    
    output$scatter_static = renderPlotly({
        GDP_suicide_scatter_allyear
    })
    
    
    # Datatable TAB
    filtered_data = reactive({
        data1 = data
        data1 = data1 %>% 
            filter(gdp_per_capita %in% input$dt_gdp[1]:input$dt_gdp[2]) %>% 
            filter(continent %in% input$dt_continent) %>%
            filter(age %in% input$dt_age) %>% 
            filter(sex %in% input$dt_sex) %>% 
            filter(year %in% input$dt_year[1]:input$dt_year[2])
        data1
    })
    
    output$datatable = DT::renderDataTable({
        data1 = filtered_data()
        data1
    })
    
    output$download_data = downloadHandler(
        filename = 'suicide_data.csv',
        content = function(file) {
            data1 = filtered_data()
            write.csv(data1, file, row.names = F)
        }
    )
    
    
    
    
    
}

shinyApp(ui, server)

