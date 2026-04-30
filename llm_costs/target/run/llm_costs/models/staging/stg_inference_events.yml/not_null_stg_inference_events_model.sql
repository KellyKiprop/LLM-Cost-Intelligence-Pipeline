
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select model
from "defaultdb"."analytics"."stg_inference_events"
where model is null



  
  
      
    ) dbt_internal_test