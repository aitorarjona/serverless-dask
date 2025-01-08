# RapiDask
___
**RapiDask** is a modification of the Dask scheduler that improves autoscaling, making it much more resource-efficient for large but sporadic jobs, such as interactive or exploratory data analytics. RapiDask deploys workers exactly when they are needed - when a user submits a job - with just the right amount of workers to meet the job's demand, and removes them once the job is complete, ensuring that no resources are allocated when there are no active jobs.
___
