# README

Under the given website ([https://getlatka.com/](https://getlatka.com/)), there were two main data that seemed logical to be scraped, the SaaS Companies dataset and Blog Section data.

As all the unstructured data was unfruitful for scraping

I’ve used beautifulsoup library for scraping

I’ve implemented parallel processing by dividing the scraping processes into multiple subprocesses

# saas_database.py:

Contains the saas companies database scrapper. 

For demo, I’ve scraped 800/32749 pages. Each page consists of information on 25 companies.

I’ve divided it into 16 subprocesses to utilise the power of multiprocessing. 

As my device is 8-threaded, now after implementing multiprocessing, the time nearly will be reduced to 1/8.

Output of this file is a saas_data.json file which contains data of 20,000 comapnies that was scraped within 1 minute

Output file is saas_data.json

# blog_dataset

Contains the blog section scrapper. 

For demo, I’ve scraped 50/54 pages. Each page contains 10 blogs.

I’ve divided it into 10 subprocesses to utilise the power of multiprocessing. 

As my device is 8-threaded, now after implementing multiprocessing, the time will nearly be reduced to 1/8.

Output of this file is a saas_data.json file which contains data of 500 comapnies that was scraped within 1 minute

Output file is blog_data.json