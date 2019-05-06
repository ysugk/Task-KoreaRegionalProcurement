library(tidyverse)
library(rvest)
library(curl)
library(jsonlite)
library(microbenchmark)

base_url <- "https://openapi.gg.go.kr/ContractCustmzInqStus/"
operation <- "getDataSetOpnStdScsbidInfo?"
key <- "ServiceKey=L7xQw3bD8No7B%2FH73Up%2BZLtAbZmkUSyYAKrq3R6k%2FdgWrByCO3j9UtKeOLpODasZnWovQi1QP5y3RtYCNk2v%2Bw%3D%3D"

date_list <- seq.Date(from = lubridate::ymd("20170524"),
                      to = lubridate::ymd("20190101"),
                      by = "1 day")

begin_list <- date_list[1:(length(date_list) - 1)] %>%
  str_remove_all("-")
end_list <- (date_list[2:length(date_list)] - 1) %>%
  str_remove_all("-")

#begin <- begin_list[20]
#end <- end_list[20]

#numOfRows <- "numOfRows=10"
#pageNo <- "pageNo=1"
#bsnsDivCd <- "bsnsDivCd=3"
#opengBgnDt <- paste0("opengBgnDt=", begin, "0000")
#opengEndDt <- paste0("opengEndDt=", end, "2359")
#type <- "type=json"

#option <- paste(pageNo, numOfRows, bsnsDivCd, opengBgnDt, opengEndDt, type, key, sep = "&")
#url <- paste0(base_url, operation, option)
#json <- fromJSON(url)
#item_list <- json$response$body$items %>%
# names() %>%
#str_to_lower()

item_list <- c( "bidntceno"           ,   "bidntceord"          ,   "bidntcenm"             ,
                "bsnsdivnm"          ,    "cntrctcnclssttusnm" ,    "cntrctcnclsmthdnm"    , 
                "bidwinrdcsnmthdnm"  ,    "ntceinsttnm"        ,    "ntceinsttcd"          , 
                "dmndinsttnm"        ,    "dmndinsttcd"        ,    "sucsflwstlmtrt"       , 
                "presmptprce"        ,    "rsrvtnprce"         ,    "bssamt"               , 
                "opengdate"          ,    "opengtm"            ,    "opengrsltdivnm"       , 
                "opengrank"          ,    "bidprccorpbizrno"   ,    "bidprccorpnm"         , 
                "bidprccorpceonm"    ,    "bidprcamt"          ,    "bidprcrt"             , 
                "bidprcdate"         ,    "bidprctm"           ,    "sucsfyn"              , 
                "dqlfctnrsn"         ,    "databssdate"           )

get_totalcount <- function(begin, end){
  numOfRows <- "numOfRows=10"
  pageNo <- "pageNo=1"
  bsnsDivCd <- "bsnsDivCd=3"
  opengBgnDt <- paste0("opengBgnDt=", begin, "0000")
  opengEndDt <- paste0("opengEndDt=", end, "2359")
  type <- "type=json"
  
  option <- paste(pageNo, numOfRows, bsnsDivCd, opengBgnDt, opengEndDt, type, key, sep = "&")
  url <- paste0(base_url, operation, option)
  json <- fromJSON(url)
  
  json$response$body$totalCount
}

extract_item <- function(list, items){
  items %>%
    html_nodes(list) %>%
    html_text()
}

page_crawl_multiFetch <- function(N, begin, end){
  print(paste("Processing page", "1", "-", N, sep = " "))
  tryCatch(
    {
      pageNo <- paste0("pageNo=", 1:N)
      numOfRows <- "numOfRows=999"
      bsnsDivCd <- "bsnsDivCd=3"
      opengBgnDt <- paste0("opengBgnDt=", begin, "0000")
      opengEndDt <- paste0("opengEndDt=", end, "2359")
      
      option <- paste(pageNo, numOfRows, bsnsDivCd, opengBgnDt, opengEndDt, key, sep = "&")
      url <- paste0(base_url, operation, option)
      
      pool <- new_pool(host_con = 5)
      data <- vector("list", N) %>%
        set_names(url)
      
      cb <- function(req){
        cat("done:", req$url, ": HTTP:", req$status, "\n")
        items <- rawToChar(req$content) %>%
          read_html() %>%
          html_nodes("item")
        
        df <- map_dfc(item_list, extract_item, items) %>%
          set_names(item_list)
        
        data[[req$url]] <<- df
      }
      
      walk(url, curl_fetch_multi, done = cb, pool = pool)
      out <- multi_run(pool = pool)
      
      df <- bind_rows(data)
      
      print(paste("Success to crawl", nrow(df), "X", ncol(df), "observations", sep = " "))
      df
    }, error=function(e){NULL})
}

page_crawl_futureMap <- function(n, begin, end){
  print(paste("Processing page", n, sep = " "))
  tryCatch(
    {
      pageNo <- paste0("pageNo=", n)
      numOfRows <- "numOfRows=999"
      bsnsDivCd <- "bsnsDivCd=3"
      opengBgnDt <- paste0("opengBgnDt=", begin, "0000")
      opengEndDt <- paste0("opengEndDt=", end, "2359")
      
      option <- paste(pageNo, numOfRows, bsnsDivCd, opengBgnDt, opengEndDt, key, sep = "&")
      url <- paste0(base_url, operation, option)
      
      items <- read_html(url) %>%
        html_nodes("item")
      
      df <- map_dfc(item_list, extract_item, items) %>%
        set_names(item_list)
      
      print(paste("Success to crawl", nrow(df), "X", ncol(df), "observations", sep = " "))
      df
    }, error=function(e){NULL})
}

month_crawl_multiFetch <- function(begin, end){
  Sys.sleep(1)
  print(paste("Now Processing period", begin, "-", end, sep = " "))
  totalCount <- get_totalcount(begin, end)
  N <- ceiling(totalCount/999)
  
  if(N == 0){
    return(NULL)
  }
  
  df <- page_crawl(N, begin, end)
  
  if(totalCount != nrow(df)){
    break
  }
  
  write_excel_csv(df, paste0("build/temp/awardedConstStandard/", begin, ".csv"))
}

month_crawl_futureMap <- function(begin, end){
  Sys.sleep(1)
  print(paste("Now Processing period", begin, "-", end, sep = " "))
  totalCount <- get_totalcount(begin, end)
  N <- ceiling(totalCount/999)
  
  if(N == 0){
    return(NULL)
  }
  
  future::plan("multicore")
  df <- furrr::future_map_dfr(1:N, page_crawl_futureMap, begin, end, .progress = TRUE)
  
  if(totalCount != nrow(df)){
    write_excel_csv(df, paste0("build/temp/awardedConstructStandard/", begin, "_error", ".csv"))
  } else {
    write_excel_csv(df, paste0("build/temp/awardedConstructStandard/", begin, ".csv"))
  }
}

walk2(begin_list, end_list, month_crawl_futureMap)
