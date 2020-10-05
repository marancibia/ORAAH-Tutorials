#######################################################
# ORAAH Tutorial 002 - Getting Started with HIVE,      
# IMPALA and Spark Dataframes                           
#                                                       
# Simple ORAAH 2.8.x Introduction                      
#                                                      
# Accessing Data stored as HIVE tables, using the      
# Transparency Layer to process data via HIVE and      
# IMPALA, executing queries, loading data into and     
# from HDFS, loading data into and from R data frames, 
# executing HQL queries, loading data into Spark       
# DataFrames, load from from HIVE and manipulate the   
# Spark DataFrames.                                    
#                                                      
# Copyright (c) 2020 Oracle Corporation                              
# The Universal Permissive License (UPL), Version 1.0                
#                                                                    
# https://oss.oracle.com/licenses/upl/  
########################################################

# Once inside the R Session, either on a Terminal running R
# or inside an RStudio Server session, the following command loads
# the ORAAH library and all its necessary components
library(ORCH)


# The first time the libraries load it should take some time testing the
# appropriate HDFS, HIVE and other environment conditions. The initializations
# after that should be faster.

# If there are problems with the configuration:
# On a BDA or BDCS, make sure the file /usr/lib64/R/etc/RBDAprofiles/Renviron.site 
# is the correct one for your CDH and Spark releases.
# On BDC and on DIY Cloudera or Hortonworks clusters, the file needs to be
# one level up: /usr/lib64/R/etc/Renviron.site

# The ORAAH Installation Guide contains sample Renviron.site files for
# different configurations
# https://www.oracle.com/technetwork/database/database-technologies/bdc/r-advanalytics-for-hadoop/documentation/index.html

# Also make sure the user that is running the commands has an available
# home folder in HDFS: /user/my-user-name

# When using RStudio Server on a BDA or BDCS, you might need to copy
# the Renviron.site file to one level up as well, to /usr/lib64/R/etc

# On any environment, when using RStudio Server add the following line to /etc/rstudio/rserver.conf
# (if you are not using Cloudera, you need to find the proper folder for your ../hadoop/lib/native)
# rsession-ld-library-path=/usr/java/latest/jre/lib/amd64/server:/usr/lib64/R/lib:/opt/cloudera/parcels/CDH/lib/hadoop/lib/native:/usr/lib64/R/port/Linux-X64/lib

# ORAAH's HIVE connectivity will use the HIVE Server2 Thrift Port, usually 10000 (hive.server2.thrift.port setting in Cloudera Manager).

# Connect to HIVE using the default port. Password is not necessary since the OS user would have been authorized access to
# HDFS and HIVE
ore.connect(type='HIVE',host='XXXXXXXXX',user='XXXXXX', port='10000', all=TRUE )

# In case of a Keberized Cluster, additional settings are needed:
# ore.connect(type='HIVE',
#             host='XXXXXXXXXX',
#             ssl='true', 
#             sslTrustStore='/opt/cloudera/security/jks/testbdcs.truststore', 
#             principal='hive/cfclbv3873@BDACLOUDSERVICE.ORACLE.COM',
#             all=TRUE )

# Optionally we can ignore the difference between orders in HIVE and local R
options("ore.warn.order" = FALSE)

# List current Tables available for the connection
ore.ls()

# We will create a random dataset and push it to HIVE for the next operations

# Creating a random R Data Frame with 25 columns and 100 k rows
nCols <- 25
nRows <- 100000
simulated_data <- data.frame(cbind(id=(1:nRows),matrix(runif(nCols*nRows), ncol=nCols)))

# Verify the size of the local dataframe
dim(simulated_data)

# Pushing the simulated data to HDFS, keeping a local reference to it
# (some 6s depending on Cluster power) 
hdfs_simul_data <- hdfs.put(simulated_data, dfs.name = 'simul_data', overwrite = TRUE)

# Load the HDFS data into a HIVE Table
# First we delete the table if it already exists
if (ore.exists('simul_table')) ore.drop(table='simul_table')

# Then we point HIVE to the HDFS data and register it as a table
hdfs.toHive(hdfs_simul_data, table = "simul_table")

# When we list the current Tables available, we should now see the "simul_table"
ore.ls()

# Let's check the contents of the Table in HIVE
# (some 28s depending on Cluster power) 
head(simul_table)

# Now create a new temporary view, based on the current simul_table
temp_table <- simul_table

# We are going to alter the temporary view via "Transparency Layer"
# Transparency layer functions include creating new variables, 
# and generating Aggregation Statistics
# Create a new column named "new_hive_var1" based on columns v2 and v3
# and a new column named "new_hive_var2" based on columns v5 and v6
temp_table$new_hive_var1 <- 1*(((temp_table$v2 + temp_table$v3)/2)>0.5)
temp_table$new_hive_var2 <- 1*(((temp_table$v5 + temp_table$v6)/2)>0.5)

# Check that the new column exists at the end of the temporary view
# (some 28s depending on Cluster power) 
head(temp_table)

# Time to count the number of Rows in the table via HIVE
# (some 28s depending on Cluster power) 
system.time({siz <- nrow(temp_table);print(formatC( siz,format = "fg",big.mark = ",")) })

# Create a new physical Table from our manipulated view
# (some 17s depending on Cluster power) 
if (ore.exists('simul_table_extra')) ore.drop(table='simul_table_extra')
ore.create(temp_table, table='simul_table_extra')

#################################
#  Statistics and Aggregations  #
#################################

# Specialized ORAAH Summary function for HIVE
# we can request several statistics to be computed
# we can use either the temp_table or the simul_table_extra table
# (some 1m30s depending on Cluster power) 
orch.summary(simul_table_extra, var = c("v2", "v3", "v4", "v5"),
             class = c("new_hive_var1"),
             stats = c("min","mean", "stddev","max"), order = c("type", "class"))

# Show the help for the orch.summary
?orch.summary

# Using the Transparency Layer function "aggregate"
# (less than 60s depending on Cluster power) 
system.time({agg <- aggregate( simul_table_extra$v3,
                               by = list(simul_table_extra$new_hive_var1),
                               FUN = mean)
names(agg) <- c("Attribute 1","Average for column v3")
print(agg)
})

# Adding an additional Factor attribute to the aggregation
# (less than 60s depending on Cluster power) 
system.time({agg <- aggregate( simul_table_extra$v3,
                               by = list(simul_table_extra$new_hive_var1,
                                         simul_table_extra$new_hive_var2),
                               FUN = mean)
names(agg) <- c("Attribute 1","Attribute 2","Average for column v3")
print(agg)
})

# Using the Transparency Layer function "table"
# for Cross-tabulation of two factor columns
# (less than 60s depending on Cluster power) 
system.time({tab <- table('Variable 1'=simul_table_extra$new_hive_var1,
                          'Variable 2'=simul_table_extra$new_hive_var2)
print(tab)
})

# Using the Transparency Layer function "colMeans" for example
# to check the Average values of all columns at once
# (less than 60s depending on Cluster power) 
colMeans(simul_table_extra)


# Using a subset of the data
# We can create a new view that is a filter on the original using subset
new_temp_table <- subset(simul_table_extra, 
                         new_hive_var1==1, 
                         select = c("v2", "v3", "v4", "v5", 
                                    "new_hive_var1","new_hive_var2"))

# Check new limited dataset
# (some 28s depending on Cluster power) 
head(new_temp_table)

# Bring the subset of data from HIVE to local memory for additional processing
# (some 13s depending on Cluster power) 
local_subset_of_data <- ore.pull(new_temp_table)

# Summary statistics from the local data for example
summary(local_subset_of_data)


#################################
#  Working with Cloudera IMPALA #
#################################

# ORAAH's IMPALA connectivity will use IMPALA's Daemon HiveServer2 Port, usually 21050 (hs2_port setting in Cloudera Manager)
# The ore.connect() command will automatically disconnect any previous connections
# With the all=FALSE option, we are asking not to sync all the tables with the environment, and we will have
# to specify the tables we want to work with manually using an ore.sync, or upload new tables
ore.connect(type='IMPALA',host='XXXXXXXXXX',port='21050',user='XXXXXXX', all=FALSE)

# In the case of a Kerberized Cluster  
# ore.connect(type='IMPALA', 
#             port='21050', 
#             AuthMech='1', 
#             KrbRealm='BDACLOUDSERVICE.ORACLE.COM', 
#             KrbHostFQDN='XXXXXXXXX', 
#             KrbServiceName='impala', 
#             all=TRUE)

# Optionally we can ignore the difference between orders in IMPALA and local R
options("ore.warn.order" = FALSE)

# We can resync and reuse the original table created in the HIVE Session above
ore.sync(table='simul_table')
ore.attach()
ore.ls()

# Or we can create a new table and overwrite the old one
# Let's drop the old table
ore.drop(table='simul_table')

# Creating a random R Data Frame with 25 columns and 100 k rows
nCols <- 25
nRows <- 100000
simulated_data <- data.frame(cbind(id=(1:nRows),matrix(runif(nCols*nRows), ncol=nCols)))

# Verify the size of the local dataframe
dim(simulated_data)

# Load the simulated into IMPALA
# (some 28s depending on Cluster power) 
ore.create(simulated_data, table='simul_table', overwrite = TRUE)

# Now create a new temporary view, based on the current simul_table
temp_table <- simul_table

# We are going to alter the temporary view via "Transparency Layer"
# Transparency layer functions include creating new variables, 
# and generating Aggregation Statistics
# Create 2 new columns named "new_impala_var1" and "new_impala_var2"
# based on columns v2 and v3
temp_table$new_impala_var1 <- 1*(((temp_table$v2 + temp_table$v3)/2)>0.5)
temp_table$new_impala_var2 <- 1*(((temp_table$v5 + temp_table$v6)/2)>0.5)

# Check that the new columns exists at the end of the temporary view
# (some 1s depending on Cluster power) 
head(temp_table)

# Time to count the number of Rows in the table via IMPALA
# (less than 1s depending on Cluster power) 
system.time({siz <- nrow(temp_table);print(siz) })

# Using the Transparency Layer function "aggregate"
# (less than 2s depending on Cluster power) 
system.time({agg <- aggregate( temp_table$v3,
                               by = list(temp_table$new_impala_var1),
                               FUN = mean)
             names(agg) <- c("Attribute 1","Average for column v3")
             print(agg)
})

# Adding an additional Factor attribute to the aggregation
# (less than 2s depending on Cluster power) 
system.time({agg <- aggregate( temp_table$v3,
                               by = list(temp_table$new_impala_var1,
                                         temp_table$new_impala_var2),
                               FUN = mean)
names(agg) <- c("Attribute 1","Attribute 2","Average for column v3")
print(agg)
})

# Using the Transparency Layer function "table"
# for Cross-tabulation of two factor columns
# (less than 2s depending on Cluster power) 
system.time({tab <- table('Variable 1'=temp_table$new_impala_var1,
                          'Variable 2'=temp_table$new_impala_var2)
             print(tab)
})

# Using the Transparency Layer function "colMeans" for example
# to check the Average values of all columns at once
# (less than 2s depending on Cluster power) 
colMeans(temp_table)

# Using a subset of the data
# We can create a new view that is a filter on the original using subset
new_temp_table <- subset(temp_table, 
                         new_impala_var1==1, 
                         select = c("v2", "v3", "v4", "v5", 
                                    "new_impala_var1", "new_impala_var2"))

# Check new limited dataset
# (less than 1s depending on Cluster power) 
head(new_temp_table)

# Bring the subset of data from HIVE to local memory for additional processing
# (less than 2s depending on Cluster power) 
local_subset_of_data <- ore.pull(new_temp_table)

# Run a local histogram of the producrt of 2 of the random variables
hist(local_subset_of_data$v5*local_subset_of_data$v4,col='red',breaks=50)



#######################################
#  Manipulating HIVE Tables via Spark #
# and using Spark SQL and Spark DF    #
#######################################

# Create a Spark Session, disconnecting any previous one if exists
# remember to add the option enableHive= TRUE to have access to the
# HIVE Metadata from Spark. This option is not needed if you are 
# already connected to HIVE via ore.connect(...)
if (spark.connected()) spark.disconnect()
spark.connect(master='yarn',memory='4g', enableHive = TRUE)

# ORAAH also supports master='local[*]', where the * can be replaced
# by the number of cores to be used by the local Spark Session
# It also supports a specific master='spark://...' Standalone Spark Cluster

# Querying HIVE tables using Spark (available starting in ORAAH 2.8.0)
# Check available Databases in HIVE from Spark with Spark SQL
queryResult <- orch.df.sql('show databases')
# Collect the results locally (if the results can be collected)
orch.df.collect(queryResult)

# Execute a Show Tables. This specifically requires the show() 
# command on the result to properly print. The default are 20 rows, 
# but more can be printed by using $show(100L) for example for 
# 100 rows. The "L" is necessary after the number.
queryResult <- orch.df.sql('show tables')
queryResult$show(50L)

# Execute a Show Tables, and list the database and tableName
# To be compatible with orch.df.collect, we need to select
# specific columns
queryResult <- orch.df.sql('show tables')
res <- queryResult$selectExpr(c("database","tableName"))
orch.df.collect(res)

# Let's review only tables available in the Database "default"
queryResult <- orch.df.sql('show tables in default')
queryResult$show(50L)

# Execute a Simple Count Query on the table simul_table_extra. 
# We can use show() to print the result, or it can be collected to R
queryResult <- orch.df.sql('select count(*) from default.simul_table_extra')
queryResult$show()
# or orch.df.collect(queryResult)

# Execution is lazily loaded by Spark, so only when we actually request the
# orch.df.collect() function to bring the result locally, the Spark job is 
# going to run
# (the first time might take 10s, if ran again it might take less than 1s)
system.time({
  queryResult <- orch.df.collect(orch.df.sql('select count(*) from default.simul_table_extra'))
  numRows <- formatC( unlist(queryResult), format = "fg",big.mark = ",")
  print(paste0('Number of rows via Spark SQL: ',numRows))
})

# Small benchmark between HIVE MapRed and SparkSQL
# Using ORAAH Transparency Layer for HIVE (Map Red)
# Connecting to HIVE directly
ore.connect(type='HIVE',host='XXXXXXXXX',user='XXXXXXX', port='10000', all=TRUE )

# Testing HIVE performance
# (about 26s)
system.time({numRows <- nrow(simul_table_extra)
             print(paste0('Number of rows via HIVE: ',numRows))
  })

# Using ORAAH Spark SQL connection
# (the first time might take 10s, if ran again it might take less than 1s)
system.time({
  queryResult <- orch.df.collect(orch.df.sql('select count(*) from default.simul_table_extra'))
  numRows <- formatC( unlist(queryResult), format = "fg",big.mark = ",")
  print(paste0('Number of rows via Spark SQL: ',numRows))
})

# We can also load the HIVE data into a Spark DataFrame for processing directly
# in-memory with SparkDF functions and Spark SQL
simul_table_df <- ORCHcore:::.ora.getHiveDF(table='simul_table_extra')

# The simul_table_df is of Class Java Reference
class(simul_table_df)

# When called directly, it will show that it is a Java-Object, a proxy
# to the Spark DataFrame
simul_table_df

# And we can show the schema of the Spark DataFrame by using 
# the $printSchema function
simul_table_df$printSchema()

# For increased performance, we can ask Spark to persist the Spark DataFrame
# This function should be used when we expect to have enough memory to hold
# the compressed data.  If data cannot entirely fit in memory, Spark will
# automatically spill to disk when needed
orch.df.persist(simul_table_df, storageLevel = "MEMORY_ONLY", verbose = TRUE)

# We can see the first 6 records of the file with the show() function
simul_table_df$show(6L)

# There are many functions associated to this type of object
# To count the rows, one can also use $count()
# (because the data is pinned to memory, it should run in less than 1s)
system.time({
  num_rows <- formatC( simul_table_df$count() ,format = "fg",big.mark = ",")
  print(paste0("Number of Rows: ",num_rows))
})


#####################################################
# Basic Statistics with Spark DF                    #
# Correlations between columns on a Spark DataFrame #
#####################################################

# We will extract all numerical columns from the Spark DataFrame, using
# the built-in function $numericColumns
# We are capturing the output from that functions and then transforming
# the output for a final string vector called "allNumericalVars"
tmp_nums <- capture.output(simul_table_df$numericColumns())
tmp_extr <- gsub(".*\\(\\s*|\\).*", "", tmp_nums)
tmp_split <- strsplit(tmp_extr,',')
allNumericalVars <- unlist(lapply(tmp_split, function(x) trimws(gsub( "#.*$", "", x ))))

# Review the final string vector
allNumericalVars

# Let's remove the "id" column from the list since correlations
# with the ID do not make sense
allNumericalVars <- allNumericalVars[!allNumericalVars %in% "id"]

# We then invoke the statistics interface ($stat) on the Spark DataFrame
simul_stats <- simul_table_df$stat()

# We request that, for each Numerical column, the Correlation be executed against the column "v2"
# using R's sapply to repeat the function $corr
# (it should take less than 3s depending on Cluster performance)
allNumCorr <- sapply(allNumericalVars, function(x) simul_stats$corr("v2",x), simplify = TRUE)

# Sort the Correlations in descending order by the absolute value
sortedAbsCorr <- allNumCorr[sort(abs(allNumCorr),decreasing = TRUE, index.return = TRUE)$ix]

# Print the resulting correlations
# We expect very small numbers since these are numbers randomly generated
print(formatC( sortedAbsCorr ,format = "fg", digits = 6))

# Simple Cross-Tabulation between the two binary columns

# We can reuse the same "statistics" already built using the 
# $stat function, and call the $crosstab function on it
xtab_simul_local <- orch.df.collect(simul_stats$crosstab("new_hive_var1",
                                                        "new_hive_var2"))
names(xtab_simul_local)[1] <- 'Var1 v Var2 >'
xtab_simul_local

########################################
# Creating Subsets of Spark DataFrames #
########################################

# Create a Subset of the original Spark DataFrame simul_table_df 
# by selecting a few columns using the function $selectExpr()
subset_table_df <- simul_table_df$selectExpr(c("id","v2","v3","v4"))

# Show the resulting schema for the subset_table_df Spark DataFrame
subset_table_df$printSchema()

# Show the first 6 records of the bset_table_df Spark DataFrame
subset_table_df$show(6L)

# The other option is to select the desired columns directly in
# the Spark SQL query
# Execute a Simple Count Query. We can use show() to print the result, 
# or it can be collected to R
subset_query_df <- orch.df.sql('select id, v2, v3, v4 from default.simul_table_extra')
subset_query_df$show(6L)

##############################
# Combining Spark DataFrames #
##############################

# Two Spark dataFrames with the same columns can be appended with the
# union() function on one of them
combined_subsets <- subset_table_df$union(subset_query_df)

# The structure of the combined Spark DF should have same 4 attributes
combined_subsets$printSchema()

# Verify the number of records of the combined Spark DF
# It should now be twice as large as the original
print(paste0('Number of records on the combined Spark DF: ',
             formatC( combined_subsets$count() ,
                      format = "fg",
                      big.mark = ",", 
                      digits = 8)))


# For joining Spark DataFrames, the easiest method is through Spark SQL
# Make sure the tables you want to join are available for Spark SQL
orch.df.sql('show tables')$show(50L)

# We can see that the original simul_table is there, but the
# temporary Spark DF we created called "subset_query_df" is not
# available yet for querying

# We need to register the subset_query_df as a temporary view for 
# the Spark SQL engine be able to see it
orch.df.createView(data = subset_query_df, viewName = "subset_query")

# Now there should be a new view (outside of the "default" database) 
# available for processing called subset_query
orch.df.sql('show tables')$show(50L)

# Now we can join the original table with the subset view
joined_table_df <- orch.df.sql('select simul_table.id, simul_table.v12 simv12, 
                                simul_table.v14 simv14, subset_query.v2 subv2, 
                                subset_query.v3 subv3, subset_query.v4 subv4
                                from default.simul_table, subset_query 
                                where simul_table.id == subset_query.id')

# The schema of the newly created temporary view is
joined_table_df$printSchema()

# And the data contents can be seen with
joined_table_df$show(6L)

# Finally we can also bring the Spark DF to the local memory
# in order to perform any other R processing on it
joined_table_local <- orch.df.collect(joined_table_df)

# Now with the local R dataframe all normal R functions apply
hist(joined_table_local$simv12, col='red', breaks=50)










####################################################
## MANAGING LARGER DATASETS WITH IMPALA AND SPARK ##
####################################################

# ORAAH's IMPALA connectivity will use IMPALA's Daemon HiveServer2 Port, usually 21050 (hs2_port setting in Cloudera Manager)
# The ore.connect() command will automatically disconnect any previous connections
# With the all=FALSE option, we are asking not to sync all the tables with the environment, and we will have
# to specify the tables we want to work with manually using an ore.sync, or upload new tables
ore.connect(type='IMPALA',host='cfclbv3872',port='21050',user='oracle', all=FALSE)

# In the case of a Kerberized Cluster  
# ore.connect(type='IMPALA', 
#             port='21050', 
#             AuthMech='1', 
#             KrbRealm='BDACLOUDSERVICE.ORACLE.COM', 
#             KrbHostFQDN='cfclbv3872', 
#             KrbServiceName='impala', 
#             all=TRUE)

# Optionally we can ignore the difference between orders in IMPALA and local R
options("ore.warn.order" = FALSE)

# Let's syncronize a larger Table from the 
# Allstate Claim Prediction Challenge in Kaggle
ore.sync(table = 'allstate')
ore.attach()
ore.ls()

# Time to cound 13.4m records via IMPALA
# (less than 2s depending on Cluster power) 
system.time({siz <- nrow(allstate);print(siz) })

# Simple Cross-tabulations via transparency layer on function "table()"
# (less than 4s depending on Cluster power) 
table('Attribute Car Category'=allstate$nvcat,'Attribute Claims'=allstate$any_claim)

##########################
# Experiments with SPARK #
##########################

# Create a Spark Session, disconnecting any previous one if exists
# remember to add the option enableHive= TRUE to have access to the
# HIVE Metadata from Spark. This option is not needed if you are 
# already connected to HIVE via ore.connect(...)
# Because we are going to work with larger data, it might be asvisable
# to increase the spark.driver.memory
if (spark.connected()) spark.disconnect()
spark.connect(master='yarn',memory='9g', enableHive = TRUE,
              spark.executor.cores='3',
              spark.driver.memory='8g')

# spark.connect(master='local[12]',memory='8g', 
#               enableHive = TRUE,
#               spark.executor.cores='3',
#               spark.driver.memory='8g')
              
# Let's review only tables available in the Database "default"
# We can use the $show function to print the results
queryResult <- orch.df.sql('show tables in default')
queryResult$show(50L)

# Execute a Simple Count Query, and collect the results to R
# (around 5s depending on Cluster performance)
system.time({
  queryResult <- orch.df.collect(orch.df.sql('select count(*) from default.allstate'))
  num_rows <- formatC( unlist(queryResult) ,format = "fg",big.mark = ",")
  print(paste0("Number of Rows: ",num_rows))
})

# We can create a pointer to the Spark DataFrame with the data
allstate_df <- orch.df.sql('select * from default.allstate')

# We can try to pin the allstate Table in memory for improved performance
orch.df.persist(allstate_df, storageLevel = "MEMORY_ONLY", verbose = TRUE)

# We can run the count directly on our proxy object to the Spark DF
# (the first run it will be caching, but subsequent runs should be faster)
system.time({
  num_rows <- formatC( allstate_df$count() ,format = "fg",big.mark = ",")
  print(paste0("Number of Rows: ",num_rows))
})

# We have special functions that can return statistics for all columns
# (around 2m depending on Cluster performance)
orch.df.collect(orch.df.describe(allstate_df))


# The Statistics interface can do Correlations (as seen before) and 
# also Cross-Tabulations

# We first invoke the statistics interface ($stat) on the Spark DataFrame
all_statistics <- allstate_df$stat()

# Simple Cross-Tabulation 

# Then we can use the function $crosstab to do a cross-tabulation on
# the original allstate_df between two class columns
xtab_Result <- orch.df.collect(all_statistics$crosstab("nvcat","any_claim"))
names(xtab_Result)[1] <- 'Car Category'
print('Car Category vs Any Claims ? (0/1)')
xtab_Result

# Column Correlations

# We will extract all numerical columns from the Spark DataFrame, using
# the built-in function $numericColumns
# We are capturing the output from that functions and then transforming
# the output for a final string vector called "allNumericalVars"
tmp_nums <- capture.output(allstate_df$numericColumns())
tmp_extr <- gsub(".*\\(\\s*|\\).*", "", tmp_nums)
tmp_split <- strsplit(tmp_extr,',')
allNumericalVars <- unlist(lapply(tmp_split, function(x) trimws(gsub( "#.*$", "", x ))))

# Review the final string vector
allNumericalVars

# Let's remove the "row_id" column from the list since correlations
# with the ID do not make sense
allNumericalVars <- allNumericalVars[!allNumericalVars %in% "row_id"]

# We request that, for each Numerical column, the Correlation be 
# computed against the column "claim_amount" using R's sapply to repeat 
# the use of function $corr
allNumCorr <- sapply(allNumericalVars, function(x) all_statistics$corr("claim_amount",x), 
                     simplify = TRUE)

# Sort the Correlations in descending order by the absolute value
sortedAbsCorr <- allNumCorr[sort(abs(allNumCorr),
                                 decreasing = TRUE, 
                                 index.return = TRUE)$ix]

# Print the resulting correlations
# We expect the binary any_claim to be highly correlated since it is just
# a function of claim_amount, so we are interested in the other columns
print(formatC( sortedAbsCorr ,format = "fg", digits = 6))


