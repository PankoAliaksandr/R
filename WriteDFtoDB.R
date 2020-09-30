
####Connect to db ######
FAFunc.GetDB <- function(database = "Aktienmodell") {
  
  RODBC::odbcDriverConnect(
    connection = paste(
      "Driver={SQL Server};server=sqltara;database=",
      database,
      ";trusted_connection=yes;",
      sep = "",
      collapse = ""
    )
  )
}

vec_to_query <- function(vector_v){
  
  # Description:
  #   The function prepares a select statement to
  #   insert into a data base
  
  # Example:
  #   input:  v <- c(1,2,3,4,5)
  #   output: "Select '1','2','3','4','5'"
  
  # input: R_vec_to_sql_query(vector_v = c(T,F,NA,"ssdf"))
  # output: "Select 'TRUE','FALSE',NULL,'ssdf'"
  
  # Note: 
  #   1. In case the input vector contains NA values they
  #      are converted to NULL values
  #   2. This can be used with "Union ALL" statement to
  #   save a df into a db by row
  
  if(length(vector_v) > 0)
  {
    vector_v <- as.character(vector_v)
    n <- length(vector_v)
    quotes_v <- c(rep("'", (n-1)))
    query_part1 <- "Select "
    query_part2 <- paste0(paste0(quotes_v,vector_v,quotes_v), collapse = ",")
    query <- paste0(query_part1, query_part2)
    
    # NA to NULL
    query <- gsub(pattern = "'NA'",replacement = "NULL", x = query)
    
    return(query)  
  }
  else
  {
    return("The input is empty")
  }
  
}

df_to_query <- function(df){
  # Description:
  #   The function creates an SQL query which 
  #   can be used to save a data.frame into a DB table
  
  # Dependencies:
  #   vec_to_query()
  
  if(is.data.frame(df))
  {
    n <- nrow(df)
    df_query <- ""
    # For each row
    for (i in 1:n)
    {
      row_v <- as.character(df[i,])
      
      row_query <- vec_to_query(row_v)
      
      # Unite rows selects together in one select statement
      if (i < n)
      {
        row_query <- paste(row_query, "UNION ALL ")
        df_query = paste(df_query, row_query)
      }
      else
      {
        # This is last row, no additional UNION ALL required.
        # The last part of df_query is "UNION ALL "
        df_query = paste(df_query, row_query)
      }
    }
    return(df_query)
  }
  else
  {
    return("Input must be a data.frame")
  }
}

df_to_db <- function(df, db_table_name){
  
  # Description:
  #   This function inserts data from predefined(prepared) df
  #   into Aktienmodell.misc.SimTSManualUpdate data table
  
  # Assumptions:
  #   The input data frame must have column names and these column names must be
  #   the same as in DB table. From these names
  #   DB table structure is built in insert statement
  
  # Dependencies:
  #   df_to_query
  #   RODBC
  
  if(is.data.frame(df) &&
     is.character(db_table_name) &&
     !is.null(names(df))){
  
    print(paste("writing timeseries into", db_table_name, "..."))
    
    n <- ncol(df)
    
    tryCatch({
      
      # remove existing data for the given runID and calcMethod upfront
      del_query = paste("Delete FROM", db_table_name)
      
      con <- FAFunc.GetDB()
      RODBC::sqlQuery(con, del_query)
      RODBC::odbcClose(con)
      
      # Get a query to insert full data frame
      ins_query <- df_to_query(df)
      
      br_open_v <- c(rep("[",n))
      br_close_v <- c(rep("]",n))
      
      col_names_part <- paste0(paste0(br_open_v,names(df),br_close_v), collapse = ",")
      table_structure <- paste0("(", col_names_part, ")")
      
      # Insert in data base
      ins_query = paste0("insert into ",
                        db_table_name,
                        table_structure,
                        ins_query)
      
      con <- FAFunc.GetDB()
      z <- RODBC::sqlQuery(channel = con, query =  ins_query)
      RODBC::odbcClose(con)
      
      if (length(z) == 0)
      {
        print("Saved Successfully")
        return(0)
      }
      else
      {
        print("Save failed")
        return(z[1])
      }
    },
    error = function(e){
      print(paste0("error: ", e))
    })
  }
  else
  {
    print("At least one of the input parameters is of a wrong type")
    return("At least one of the input parameters is of a wrong type")
  }
}


# Write a data.frame object to a DB table
write_df_to_db <- function(df,
                           db_table_name = 'Aktienmodell.misc.SimTSManualUpdate',
                           mapping_v){
  # Description
  #   The functiono saves values from a data.frame into a 
  #   specified data base table. The main task is done here is
  #   to make sure the data.frame has the same structure as SQL DB table
  
  # Dependensies
  #   library(RODBC)
  
  # Input Parameters:
  #   df: data.frame object to be saved
  #   db_table_name: data base table name to save in
  #   mapping_v: is a named vector which stores values of 2 types
  #     case 1: character strings which will be used as a value for each row
  #       Ex. mapping_v["IndicatorID"] = "Percentage Weight"
  #     case 2: negative integer values (indexes of the corresponding input df column)
  #       Ex. mapping_v["MarketID"] = -1. This means that the "MarketID" column will be
  #         populated as df[,1]
  
  # Note:
  #   Negative values are used on purpose to avoid potential problems
  #   By name it is also not save: Ex IndicatorID = "Percentage Weight" and the same value has the df column name

  if(is.data.frame(df) &&
     is.character(db_table_name) &&
     is.character(mapping_v) &&
     !is.null(names(df)) &&
     !is.null(names(mapping_v))){
    
    n_rows <- nrow(df)
    n_cols <- length(mapping_v)
    
    # Create empty data.frame but with the correct shape
    df_to_upload <- as.data.frame(matrix(rep(NA, times = n_rows * n_cols),
                                         nrow = n_rows,
                                         ncol = n_cols))
    # Set column names
    names(df_to_upload) <- names(mapping_v)
    for(i in 1:length(mapping_v))
    {
      # If NA this means it is not an index, but the character name
      index_in_df <- suppressWarnings(as.numeric(mapping_v[i]))
      if(!is.na(index_in_df) && index_in_df < 0)
      {
        # Copy a column by index
        df_to_upload[,i] <- df[,abs(index_in_df)]
      }
      else
      {
        # It is not an index, but the character name
        df_to_upload[,i] <- rep(mapping_v[i], n_rows)
      }
    }
      # Write data frame into DB
      df_to_db(df = df_to_upload, db_table_name = db_table_name)
  } 
  else
  {
    print("At least one of the input parameters is of a wrong type")
    return("At least one of the input parameters is of a wrong type")
  }
}


# Test

# Connect to the Bloomberg API
bbcon <- Rblpapi::blpConnect()

ovie = c("END_DT"="20180927") 

SX5E <- Rblpapi::bds(security = 'SX5E Index', field = 'INDX_MWEIGHT', overrides = ovie)
df <- SX5E

# values are eiter user values or negative required column indexes of the data frame
# Negative values are used to distinguish betweed SimID and other possible ID values
# negative values have very low probability to cause problems
# By name it is not save also: Ex IndicatorID = "Percentage Weight" and the same value has the df column name
mapping_v <- c("4378",-1, "Percentage Weight", "Weight", "NA", as.character(Sys.Date()), -2)
names(mapping_v) <- c("SimID", "Marketid", "IndicatorID", "TS_Type", "CRNCY", "Dates", "VAR")

write_df_to_db(df = df,mapping_v = mapping_v)
