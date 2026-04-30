
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cost_usd
from "defaultdb"."analytics"."stg_inference_events"
where cost_usd is null



  
  
      
    ) dbt_internal_test