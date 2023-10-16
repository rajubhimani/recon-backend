# recon-backend

This is Reconciliation software that provide a platform where user can perform recon between two sources. It uses Amazon Athena, so user data is not needed in the same database, we can can perform recon between different database tables. Recon is configurable using APIs. 

It support diffrent table structure like 
1. Both sources can have different column names- Here user can map columns to used for match
2. Data column can has differnt values - Here user can use lookups to get both data to same ground for match

**Data can be match with below conditions:**
   - User can schedule jobs based on cron string
   - daily date wise - day ago, so here recon will run daily and pics data days ago - 1 day back or 2 day and so on
   - window - monthly, or like 4 days ago, so here recon will run daily and picks data for 4 days back from today to match
