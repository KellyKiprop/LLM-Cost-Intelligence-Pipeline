
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_cost_usd
from "defaultdb"."analytics"."fct_cost_by_team_daily"
where total_cost_usd is null



  
  
      
    ) dbt_internal_test