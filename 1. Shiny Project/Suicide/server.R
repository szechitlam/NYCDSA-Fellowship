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
    mtext("[White Color: No Data Available]",side=1,line=-1)
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

