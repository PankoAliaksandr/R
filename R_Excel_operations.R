# Description:
#   Read Excel File
# Requirements:
#   xlsx 
read_excel <- function(file_path_in, input_file, ws_name, col_ind_v){

  # Read Excel worksheet to get lb, ub, convictions, tradability...
  tryCatch({
    # Use 6 required columns (to avoid NA column)
    target_settings_df <- xlsx::read.xlsx(file = paste(file_path_in,
                                                       input_file,
                                                       sep = "",
                                                       collapse = ""),
                                          sheetName = ws_name,
                                          colIndex = col_ind_v)
    # Excel can contain empty rows this produces NAs
    target_settings_df <- stats::na.omit(target_settings_df)
  },
  error = function(e){
    print(e)
    stop(e)
  })
  
  if(class(target_settings_df) == "data.frame" &&
     nrow(target_settings_df) > 0)
  {
    # do something
  }
  else
  {
    stop("Read failed")  
  }
}

# Description:
#   Read Excel Worksheets names
# Requirements:
#   readxl
read_ws_name <- function(path){
  
  if(file.exists(path)){
    ws_v <- readxl::excel_sheets(path = path)
  }else{
    ws_v <- "Excel does not exist"
  }
  
  return(ws_v)  
}


# Description:
#   Write data.frame in Excel
# Requirements
#   xlsx
write_df_to_excel <- function(df,
                              file_path_name,
                              include_row_names = TRUE,
                              sheet_name = "R.output",
                              PutInClipboard){
  xlsx::write.xlsx(x = df,
                   file = file_path_name,
                   row.names = include_row_names,
                   sheetName = sheet_name)
  
  if(PutInClipboard)
  {
    write.table(df, "clipboard", sep = "\t", col.names = TRUE)
    print("Results are saved in Excel and copied in your clipboard")
  }
  else
  {
    print("Results are saved in Excel")
  }  
}



# Read data from Excel/csv
get_mapping_from_csv <- function(path = file.choose(),
                                 req_ws = NULL,
                                 col_names = NULL,
                                 header = TRUE){
  # Description:
  #    The function reads .csv document.
  #    The are assumptions for the doc structure
  
  # Dependencies:
  #   readxl library
  
  # Check if file exists
  if(file.exists(path))
  {
    
    # Get all available worksheets
    ws_v <- readxl::excel_sheets(path = path)
    
    if(is.null(req_ws))
    {
      # is there is no worksheet provided take the firs one
      if(length(ws_v) > 0){
        req_ws <- ws_v[1]
      }
      else{
        return("get_mapping_from_csv: There are no worksheets in the document")
      }
    }
    else
    {
      # Worksheet name is provided
      if(req_ws %in% ws_v)
      {
        # Open and read excel file
        tryCatch(expr =
        {
          df <- xlsx::read.xlsx2(file = path,
                                 sheetName = req_ws,
                                 header = header,
                                 as.data.frame = TRUE,
                                 stringsAsFactors = FALSE
          )
        },
        error = function(e){
          return(e)
        },
        finally = {
          # nothing
        }
        )
        
        if(is.data.frame(df))
        {
          
          if(length(names(df)) == length(col_names)){
            # this means that column names are provided
            # and number is the same
            colnames(df) <- col_names
          }
          else if(is.null(col_names)){
            # if no names provided, use what you have
          }
          else{
            # The length is different
            return("get_mapping_from_csv: Privided column names does not have the same lenght as in the doc")
          }
          return(df)
        }
        else
        {
          return("get_mapping_from_csv: An error occured during the .csv file reading")
        }
      }
      else
      {
        return("get_mapping_from_csv: Worksheet does not exist")
      }
    }
  }
  else
  {
    return(paste0("get_mapping_from_csv: file ", path," does not exist"))
  }
}

# Read Excel/csv file with spectial column structure
get_data_from_csv <- function(path = file.choose(),
                              req_ws = NULL,
                              header = TRUE,
                              col_names = NULL,
                              end_row = NULL,
                              start_date = NULL,
                              end_date = NULL){
  
  # Description:
  #    The function reads .csv document.
  #    The are assumptions for the doc structure
  
  # Dependencies:
  #   readxl library
  
  # Check if file exists
  if(file.exists(path))
  {
    
    # Get all available worksheets
    ws_v <- readxl::excel_sheets(path = path)
    
    if(is.null(req_ws))
    {
      # is there is no worksheet provided take the firs one
      if(length(ws_v) > 0){
        req_ws <- ws_v[1]
      }
      else{
        return("get_data_from_csv: There are no worksheets in the document")
      }
    }
    else
    {
      # Worksheet name is provided
      if(req_ws %in% ws_v)
      {
        col_classes <- NA # default value
        if(!is.null(col_names))
        {
          n <- length(col_names)
          col_classes <- c("Date", rep("numeric",(n-1) ))
        }
        else
        {
          # No column names provided
          # this number is reduced to required
          col_classes <- c("Date", rep("numeric", 10000))
          names(col_classes) <- c("date", rep(NA, 10000))
        }
        
        # Open and read excel file
        tryCatch(expr =
        {
          df <- xlsx::read.xlsx2(file = path,
                                 sheetName = req_ws,
                                 header = header,
                                 as.data.frame = TRUE,
                                 colClasses = col_classes,
                                 endRow = end_row)
        },
        error = function(e){
          return(e)
        },
        finally = {
          # nothing
        }
        )
        
        if(is.data.frame(df))
        {
          
          if(length(names(df)) == length(col_names)){
            # this means that column names are provided
            # and number is the same
            colnames(df) <- col_names
          }
          else if(is.null(col_names)){
            # if no names provided, use what you have
          }
          else{
            # The length is different
            return("get_data_from_csv: Privided column names does not have the same lenght as in the doc")
          }
          
          names(df)[1] <- "date"
          
          # Take only rows with filled date
          df <- df[is.na(df$date) == FALSE, , drop = FALSE]
          
          xts_data <- convert_to_xts(df)
          
          # Use only required range
          remain_ind_v <- which(zoo::index(xts_data) >= as.Date(start_date) &
                                  zoo::index(xts_data) <= as.Date(end_date))
          
          # For missing observations we using NA
          xts_data[is.nan(xts_data)] <- NA
          
          # Take only required period
          xts_data <- xts_data[remain_ind_v,,drop = FALSE]
          
          # Delete ticker if it contains NA only
          na_num_v <- unlist(lapply(xts_data,function(x) sum(is.na(x))))
          to_remain_v <- na_num_v != nrow(xts_data)
          to_delete_v <- na_num_v == nrow(xts_data)
          
          print("Number of NA by ticker ")
          print(na_num_v)
          print("In percent: ")
          print(na_num_v/nrow(xts_data))
          
          if(sum(to_delete_v) > 0)
          {
            cat("Next tickers was deleted since contain NA only: ")
            print(names(xts_data)[to_delete_v])
          }
          else
          {
            cat("All tickers from Excel are used")
          }
          
          xts_data <- xts_data[,to_remain_v, drop = FALSE]
          
          return(xts_data)
        }
        else
        {
          return("get_data_from_csv: An error occured during the .csv file reading")
        }
        
      }
      else
      {
        return("get_data_from_csv: Worksheet does not exist")
      }
    }
  }
  else
  {
    return(paste0("get_data_from_csv: file ", path," does not exist"))
  }
}

# Description:
#   Reads excel document and analyses it on detailed level
# Requires:
#   openxlsx
get_input_params <- function(file_name = "FAP_template.xlsm"){
  
  wb <- loadWorkbook(file = file_name)
  ws_all <- getSheets(wb)
  ws <- ws_all$sheet_name
  rows <- getRows(sheet = ws)
  
  # Pick only required columns
  # Portfolio names (2), targets(4), parameter names(8), parameter values(9)
  cells <- getCells(row = rows, colIndex = c(2,4,8,9))
  
  # iterating via cells
  for(cell in cells){
    
    col_ind <- cell$getColumnIndex()
    row_ind <- cell$getRowIndex()
    cell_value <- getCellValue(cell)
   
    # Do something 
  }
  
  return()
}



# Update Excel by cell
update_excel <- function(file_name){
  # Input Parameters:
  #   file_name: template file name
  # Assumptions:
  #   1. Strong assumption is template structure, since
  #      columns indexes are hardcoded
  
    wb <- loadWorkbook(file = "FAP_template.xlsm")
    
    ws_all <- getSheets(wb)
    ws <- ws_all$ws_name
  
    rows_v <- first_row_ind : last_row_ind
    rows <- getRows(sheet = ws, rowIndex = rows_v)
    
    cols_v <- first_cor_ind : last_cor_ind
    cells <- getCells(row = rows, colIndex = cols_v)
    
    my_value = 0
    
    # Iterate through cells
    for(cell in cells){
      
      current_col_ind <- cell$getColumnIndex()
      current_row_ind <- cell$getRowIndex()
      setCellValue(cell = cell, value = my_value)
    }
    
    saveWorkbook(wb = wb, file = "FAP_output.xlsm")
}