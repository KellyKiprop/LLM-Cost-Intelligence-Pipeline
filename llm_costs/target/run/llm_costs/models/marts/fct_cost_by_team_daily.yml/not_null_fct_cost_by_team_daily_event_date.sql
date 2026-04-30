
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_date
from "defaultdb"."analytics"."fct_cost_by_team_daily"
where event_date is null



  
  
      
    ) dbt_internal_test