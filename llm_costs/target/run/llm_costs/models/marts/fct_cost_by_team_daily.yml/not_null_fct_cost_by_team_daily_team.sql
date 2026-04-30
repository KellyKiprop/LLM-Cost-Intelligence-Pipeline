
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select team
from "defaultdb"."analytics"."fct_cost_by_team_daily"
where team is null



  
  
      
    ) dbt_internal_test