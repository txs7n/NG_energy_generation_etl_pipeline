# NG_energy_generation_etl_pipeline
An ETL data pipeline that extracts energy generation data from the TCN (Transmission Company of Nigeria) website and is automated using Apache Airflow with the synchronization of several AWS services such as an EC2 instance, AWS Lambda, and Eventbridge.

## Data
[Hourly Generation by GenCos](https://www.niggrid.org/GenerationProfile2)  

## Tools  
- Amazon EC2: To host and run ETL operations.  
- AWS Lambda: To manage the start and stop actions of your EC2 instance based on specific triggers.  
- Amazon EventBridge: To schedule and trigger AWS Lambda functions for starting and stopping ec2 instance at designated times.  
- Airflow: To orchestrate and manage data collection workflows.  
- Amazon S3: For storing the collected energy data securely in the cloud.
- Python: For data processing and modeling.  
- Pandas: Library used for data manipulation.  
- Requests: Library used to make HTTP requests to external websites.
- BeautifulSoup: Library used for parsing HTML documents.  

## Task
The goal here is to first extract historic energy generation data from the TCN website since the time they started archiving data. While the site showed they started reporting generation data since 2004, I decided to start my data collection from 2021. This is due to storage reasons and the fact that consistent daily data collection did not begin till 2021.


## Extract
After importing the necessary libraries, the next step is the extract the data from the website. In contrast to static websites, this particular one was dynamically loaded with JavaScript, requiring a user to submit form requests to query the database and return a result. Hence, for the extract process to work, I needed to simulate the interactions that a user would typically make, such as selecting dates or clicking buttons.  

So, I made an initial GET request to load the page and capture important data. I then used the data captured in the initial request to make a POST request that simulates selecting a date and clicking the "Get Generation" button. Lastly, I parsed the HTML response with BeautifulSoup to extract the table data and store in a dataframe.  

The code loops through the dates **2021/01/01 - 2024/05/10** thereby collecting 3+ years of energy generation data.

## Transform
The next step is to transform the data; essentially, clean the data to make it more suitable for storage and analysis. I implemented simple cleaning procedures to remove unnecessary columns, empty rows, and strip any inherited formatting.  

This script also checks if the dataframe is empty before proceeding with operations that assume it has data so as to avoid cases where no data is available on a page for certain dates.

## Load
For the historical 3+ years data, I loaded this as a csv file on my local machine.  

## Automation with Apache Airflow  
Sequel to collecting historical data from January 2021 to May, 2024, I decided to automate this ETL pipeline moving forward to collect generation data every day at 06:00 am UTC with the aid of scheduled DAGs on Apache Airflow.  

The ETL process follows the same logic, but instead of hosting on my local machine, I hosted Airflow in a virtual environment on an Ubuntu EC2 t2.small instance. And instead of starting from Jan 2021, the data collection in this system started on May 12, 2024.  

The DAG is set up in a way that it collects data from the previous day to the stipulated time it is scheduled to run. This arrangement is necessary because the website uploads hourly generation data by the hour. There's usually latency in data availability so a delay of 6 hours past midnight the next day is enough time for the website to have uploaded the Genco energy data of that day. 

The data collected is then loaded into an S3 bucket for storage and further analysis.  

To minimize AWS costs, I have implemented Lambda functions that are triggered by EventBridge rules to start my EC2 instance 10 minutes before the DAG's scheduled run time, allowing sufficient time for server initialization. Additionally, I set another rule to stop the instance shortly after 6 AM. Since the instance is not required to operate continuously—only during the anticipated DAG run time—this strategy proves to be both efficient and cost-effective.  

I then configured Airflow to run as a systemd service to ensure the DAGs run automatically every time the ec2 instance is powered on.  

## Other Pipeline Considerations
In this project, the ETL, i.e. Extract, Transform, and Load framework was used. Another popular consideration is ELT, i.e. Extract, Load, and Transform. I decided to use the ELT framework because my server (EC2) had enough memory to handle the data coming in from the URL within the instance. In addition to that, Airflow has XCom protocols that allow data to easily pass from one task to the other. The ELT framework can be considered when we are dealing with large volumes of data.

In this project, I opted for the more traditional ETL (Extract, Transform, and Load) approach instead of the ELT (Extract, Load, and Transform) method. This decision was influenced by the high memory capacity of the Amazon EC2 server, which could efficiently handle the temporary storage of the data volume from the source URL and the data processing needs. Additionally, using Apache Airflow's XCom feature allows for smooth data transfer between tasks in the workflow, which further supports the ETL approach. The ELT framework can be considered when handling large datasets where extracted data is first dumped in a staging area, followed by transformation within the data warehouse. 


## Review After Two Weeks
After two weeks of this DAG essentially running on autopilot, I decided to update this project with a screenshot showing the status and schedule of the DAG over the days it ran.  

![Screenshot (760)](https://github.com/txs7n/NG_energy_generation_etl_pipeline/assets/118135226/51b3ea36-d35d-4d87-9593-e13fee8ca558)


From the screenshot, we see that the run history shows a consistent pattern of successful executions. 


PS: I made some changes to the schedule time as well. Originally set to run at 06:00 UTC, I changed it to 22:00 UTC due to the high demand for AWS's compute resources in my region and availability zone during the early hours of the day. 
